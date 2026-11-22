package batch

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
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

// withModel returns the body with model replaced by the upstream model name.
func (l *inputLine) withModel(upstreamModel string) ([]byte, error) {
	body := make(map[string]json.RawMessage, len(l.Body))
	for key, value := range l.Body {
		body[key] = value
	}
	modelValue, err := common.Marshal(upstreamModel)
	if err != nil {
		return nil, err
	}
	body["model"] = modelValue
	return common.Marshal(body)
}
