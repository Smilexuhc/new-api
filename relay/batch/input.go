package batch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/relay/helper"
)

const EndpointChatCompletions = "/v1/chat/completions"

const maxCustomIdLength = 512

// inputLine is one request of a batch input file.
type inputLine struct {
	Line     int
	CustomId string
	Body     map[string]json.RawMessage
	Model    string
	// MaxOutputTokens is the output limit the request sets, or 0.
	MaxOutputTokens int
	// Choices is the number of completions the request asks for (n).
	Choices int
	// RawBody is the request body as uploaded, used to estimate its input
	// tokens.
	RawBody []byte
}

type rawInputLine struct {
	CustomId string          `json:"custom_id"`
	Method   string          `json:"method"`
	URL      string          `json:"url"`
	Body     json.RawMessage `json:"body"`
}

func lineError(line int, reason string) *Error {
	return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidLine, map[string]any{"Line": line, "Reason": reason})
}

// parseInput validates a batch input file. endpoint is empty while a file is
// uploaded and set when a batch is created from it; then every line must
// target it and name the same model.
func parseInput(content []byte, endpoint string, maxRequests int) ([]*inputLine, *Error) {
	lines := make([]*inputLine, 0)
	seen := make(map[string]int)
	model := ""
	for index, raw := range bytes.Split(content, []byte("\n")) {
		lineNumber := index + 1
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			continue
		}
		if len(lines) >= maxRequests {
			return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchTooManyRequests, map[string]any{"Max": maxRequests})
		}
		var parsed rawInputLine
		if err := common.Unmarshal(raw, &parsed); err != nil {
			return nil, lineError(lineNumber, "the line is not a JSON object")
		}
		if parsed.CustomId == "" {
			return nil, lineError(lineNumber, "custom_id is required")
		}
		if len(parsed.CustomId) > maxCustomIdLength {
			return nil, lineError(lineNumber, "custom_id is longer than 512 characters")
		}
		if first, ok := seen[parsed.CustomId]; ok {
			return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchDuplicateCustomId,
				map[string]any{"Line": lineNumber, "First": first, "CustomId": parsed.CustomId})
		}
		seen[parsed.CustomId] = lineNumber
		if !strings.EqualFold(parsed.Method, http.MethodPost) {
			return nil, lineError(lineNumber, "method must be POST")
		}
		if parsed.URL == "" {
			return nil, lineError(lineNumber, "url is required")
		}
		if endpoint != "" && parsed.URL != endpoint {
			return nil, lineError(lineNumber, "url must equal the endpoint of the batch ("+endpoint+")")
		}
		var body map[string]json.RawMessage
		if err := common.Unmarshal(parsed.Body, &body); err != nil || body == nil {
			return nil, lineError(lineNumber, "body must be a JSON object")
		}
		// Upstreams fail the whole batch over one such line.
		if parsed.URL == EndpointChatCompletions {
			var messages []json.RawMessage
			if err := common.Unmarshal(body["messages"], &messages); err != nil || len(messages) == 0 {
				return nil, lineError(lineNumber, "body.messages must be a non-empty array")
			}
		}
		line := &inputLine{Line: lineNumber, CustomId: parsed.CustomId, Body: body, RawBody: parsed.Body}
		if rawModel, ok := body["model"]; ok {
			if err := common.Unmarshal(rawModel, &line.Model); err != nil {
				return nil, lineError(lineNumber, "body.model must be a string")
			}
		}
		if rawStream, ok := body["stream"]; ok {
			var stream bool
			if err := common.Unmarshal(rawStream, &stream); err != nil || stream {
				return nil, lineError(lineNumber, "streaming is not supported in a batch")
			}
		}
		maxTokens, err := readMaxOutputTokens(body)
		if err != nil {
			return nil, lineError(lineNumber, err.Error())
		}
		line.MaxOutputTokens = maxTokens
		if line.Choices, err = readChoices(body); err != nil {
			return nil, lineError(lineNumber, err.Error())
		}
		if err := checkBodyFields(body); err != nil {
			return nil, lineError(lineNumber, err.Error())
		}
		if endpoint != "" {
			if line.Model == "" {
				return nil, lineError(lineNumber, "body.model is required")
			}
			if model == "" {
				model = line.Model
			} else if line.Model != model {
				return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchMixedModels,
					map[string]any{"Line": lineNumber, "Model": line.Model, "Expected": model})
			}
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchEmptyFile, nil)
	}
	return lines, nil
}

func readMaxOutputTokens(body map[string]json.RawMessage) (int, error) {
	best := 0
	for _, field := range []string{"max_completion_tokens", "max_tokens"} {
		raw, ok := body[field]
		if !ok || string(raw) == "null" {
			continue
		}
		var value float64
		if err := common.Unmarshal(raw, &value); err != nil || value < 0 || value > math.MaxInt32 || value != math.Trunc(value) {
			return 0, errors.New(field + " must be a non-negative integer within the max tokens limit")
		}
		limit := uint(value)
		if helper.ExceedsMaxTokensLimit(&limit) {
			return 0, errors.New(field + " must be a non-negative integer within the max tokens limit")
		}
		best = max(best, int(limit))
	}
	return best, nil
}

// maxChoices bounds n, which multiplies the output a request may bill.
const maxChoices = 8

func readChoices(body map[string]json.RawMessage) (int, error) {
	raw, ok := body["n"]
	if !ok || string(raw) == "null" {
		return 1, nil
	}
	var value float64
	if err := common.Unmarshal(raw, &value); err != nil || value < 1 || value > maxChoices || value != math.Trunc(value) {
		return 0, fmt.Errorf("n must be an integer from 1 to %d", maxChoices)
	}
	return int(value), nil
}

// batchBodyFields are the chat completion fields a batch request may set. A
// field that changes what the upstream runs or bills beyond the priced model
// and tokens, such as models, provider or plugins, is rejected, since the
// batch is priced from its model and token usage only.
var batchBodyFields = map[string]struct{}{
	"model": {}, "messages": {}, "max_tokens": {}, "max_completion_tokens": {}, "stream": {},
	"temperature": {}, "top_p": {}, "top_k": {}, "min_p": {}, "seed": {}, "stop": {}, "n": {},
	"frequency_penalty": {}, "presence_penalty": {}, "repetition_penalty": {},
	"logit_bias": {}, "logprobs": {}, "top_logprobs": {}, "response_format": {},
	"tools": {}, "tool_choice": {}, "parallel_tool_calls": {}, "functions": {}, "function_call": {},
	"reasoning": {}, "reasoning_effort": {}, "verbosity": {}, "user": {}, "metadata": {},
	// Gemini bills its search grounding as a tool; other upstreams reject it.
	"web_search_options": {},
}

// droppedBodyFields are removed before submission, as the synchronous relay
// removes them by default; a batch has no stream and its own service tier.
var droppedBodyFields = []string{"store", "stream_options", "service_tier", "safety_identifier", "prompt_cache_key", "inference_geo", "speed"}

func checkBodyFields(body map[string]json.RawMessage) error {
	for field := range body {
		if _, ok := batchBodyFields[field]; ok || slices.Contains(droppedBodyFields, field) {
			continue
		}
		return fmt.Errorf("body.%s is not supported in a batch", field)
	}
	return nil
}

// checkUpstreamFields rejects the fields an upstream would run without the
// batch billing them.
func (l *inputLine) checkUpstreamFields(channelType int) error {
	if _, ok := l.Body["web_search_options"]; ok && channelType != constant.ChannelTypeGemini {
		return errors.New("body.web_search_options is not supported in a batch on this channel")
	}
	tools, err := l.tools()
	if err != nil {
		return err
	}
	for _, tool := range tools {
		if tool.Type != "function" {
			return fmt.Errorf("tool type %q is not supported in a batch", tool.Type)
		}
	}
	return nil
}

type batchTool struct {
	Type     string `json:"type"`
	Function struct {
		Name string `json:"name"`
	} `json:"function"`
}

func (l *inputLine) tools() ([]batchTool, error) {
	raw, ok := l.Body["tools"]
	if !ok || string(raw) == "null" {
		return nil, nil
	}
	var tools []batchTool
	if err := common.Unmarshal(raw, &tools); err != nil {
		return nil, errors.New("body.tools must be an array of tools")
	}
	return tools, nil
}

// searchesWeb reports whether a Gemini request may run Google Search
// grounding: through web_search_options or the googleSearch pseudo tool.
func (l *inputLine) searchesWeb() bool {
	if _, ok := l.Body["web_search_options"]; ok {
		return true
	}
	tools, _ := l.tools()
	for _, tool := range tools {
		if tool.Function.Name == "googleSearch" {
			return true
		}
	}
	return false
}

// outputLimit is the output limit the request is sent with: its own, or
// defaultMaxOutput when it sets none.
func (l *inputLine) outputLimit(defaultMaxOutput int) int {
	if l.MaxOutputTokens > 0 {
		return l.MaxOutputTokens
	}
	return defaultMaxOutput
}

// maxOutput is the most output the request can bill: its output limit for
// each of its choices.
func (l *inputLine) maxOutput(defaultMaxOutput int) int {
	return l.outputLimit(defaultMaxOutput) * max(l.Choices, 1)
}

// upstreamBody returns the body with model replaced by the upstream model
// name. A request without an output limit gets defaultMaxOutput as max_tokens,
// so its output, and with it its charge, stays within the reservation.
func (l *inputLine) upstreamBody(upstreamModel string, defaultMaxOutput int) ([]byte, error) {
	body := make(map[string]json.RawMessage, len(l.Body))
	for key, value := range l.Body {
		body[key] = value
	}
	for _, field := range droppedBodyFields {
		delete(body, field)
	}
	modelValue, err := common.Marshal(upstreamModel)
	if err != nil {
		return nil, err
	}
	body["model"] = modelValue
	// A zero limit means none upstream, and the Gemini converter prefers a
	// zero max_completion_tokens over a set max_tokens.
	for _, field := range []string{"max_completion_tokens", "max_tokens"} {
		var value float64
		if raw, ok := body[field]; ok && (string(raw) == "null" || (common.Unmarshal(raw, &value) == nil && value == 0)) {
			delete(body, field)
		}
	}
	if l.MaxOutputTokens <= 0 && defaultMaxOutput > 0 {
		limit, err := common.Marshal(defaultMaxOutput)
		if err != nil {
			return nil, err
		}
		body["max_tokens"] = limit
	}
	return common.Marshal(body)
}
