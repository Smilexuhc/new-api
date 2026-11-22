package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

// GeminiInlineRequestLimit is the documented limit of a batch sent inline to
// models/{model}:batchGenerateContent. Larger batches need the Files API,
// which this adaptor does not use.
const GeminiInlineRequestLimit = 20 << 20

// geminiBatchLifetime is how long Gemini keeps a batch pending or running
// before it expires it.
const geminiBatchLifetime = 48 * time.Hour

// geminiAdaptor uses the Gemini API (AI Studio) batch mode. Requests are
// converted from OpenAI chat completions with the converter the synchronous
// Gemini channel uses and sent inline; inline results come back in input
// order with usageMetadata and modelVersion. Batch requests cost half the
// standard price.
type geminiAdaptor struct{}

type geminiInlinedRequest struct {
	Request  json.RawMessage `json:"request"`
	Metadata struct {
		Key string `json:"key"`
	} `json:"metadata"`
}

type geminiOperation struct {
	Name     string `json:"name"`
	Done     bool   `json:"done"`
	Metadata struct {
		State      string `json:"state"`
		BatchStats struct {
			RequestCount           string `json:"requestCount"`
			SuccessfulRequestCount string `json:"successfulRequestCount"`
			FailedRequestCount     string `json:"failedRequestCount"`
		} `json:"batchStats"`
		Output *geminiBatchOutput `json:"output"`
	} `json:"metadata"`
	Response *geminiBatchOutput `json:"response"`
	Error    *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type geminiBatchOutput struct {
	ResponsesFile    string `json:"responsesFile"`
	InlinedResponses *struct {
		InlinedResponses []geminiInlinedResponse `json:"inlinedResponses"`
	} `json:"inlinedResponses"`
}

type geminiInlinedResponse struct {
	Response json.RawMessage `json:"response"`
	Error    *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Metadata struct {
		Key string `json:"key"`
	} `json:"metadata"`
}

func (a *geminiAdaptor) DefaultPriceRatio() float64 {
	return 0.5
}

func (a *geminiAdaptor) SupportsEndpoint(endpoint string) bool {
	return endpoint == EndpointChatCompletions
}

func (a *geminiAdaptor) ConvertRequest(c *gin.Context, info *relaycommon.RelayInfo, request *dto.GeneralOpenAIRequest) ([]byte, error) {
	result, err := service.ConvertRequest(c, info, types.RelayFormatGemini, request)
	if err != nil {
		return nil, err
	}
	return common.Marshal(result.Value)
}

func (a *geminiAdaptor) headers(conn *Connection) map[string]string {
	return map[string]string{"x-goog-api-key": conn.Key}
}

func (a *geminiAdaptor) apiURL(conn *Connection, path string) string {
	return strings.TrimRight(conn.BaseURL, "/") + "/v1beta/" + path
}

func buildGeminiCreateBody(req *SubmitRequest) ([]byte, error) {
	requests := make([]geminiInlinedRequest, 0, len(req.Items))
	for _, item := range req.Items {
		inlined := geminiInlinedRequest{Request: item.Body}
		inlined.Metadata.Key = item.CustomId
		requests = append(requests, inlined)
	}
	body := map[string]any{
		"batch": map[string]any{
			"displayName": req.BatchId,
			"inputConfig": map[string]any{
				"requests": map[string]any{"requests": requests},
			},
		},
	}
	return common.Marshal(body)
}

func (a *geminiAdaptor) Submit(ctx context.Context, conn *Connection, req *SubmitRequest) (*UpstreamBatch, error) {
	body, err := buildGeminiCreateBody(req)
	if err != nil {
		return nil, err
	}
	if len(body) > GeminiInlineRequestLimit {
		return nil, ErrGeminiInlineTooLarge
	}
	modelName := strings.TrimPrefix(req.UpstreamModel, "models/")
	data, err := doUpstream(ctx, conn, http.MethodPost, a.apiURL(conn, "models/"+modelName+":batchGenerateContent"), a.headers(conn), body)
	if err != nil {
		return nil, err
	}
	batch, err := parseGeminiOperation(data)
	if err != nil {
		return nil, err
	}
	batch.ExpiresAt = time.Now().Add(geminiBatchLifetime).Unix()
	return batch, nil
}

func (a *geminiAdaptor) Retrieve(ctx context.Context, conn *Connection, upstreamId string) (*UpstreamBatch, error) {
	data, err := doUpstream(ctx, conn, http.MethodGet, a.apiURL(conn, upstreamId), a.headers(conn), nil)
	if err != nil {
		return nil, err
	}
	return parseGeminiOperation(data)
}

func (a *geminiAdaptor) Cancel(ctx context.Context, conn *Connection, upstreamId string) (*UpstreamBatch, error) {
	if _, err := doUpstream(ctx, conn, http.MethodPost, a.apiURL(conn, upstreamId+":cancel"), a.headers(conn), []byte("{}")); err != nil {
		return nil, err
	}
	return a.Retrieve(ctx, conn, upstreamId)
}

func (a *geminiAdaptor) Delete(ctx context.Context, conn *Connection, upstreamId string) error {
	_, err := doUpstream(ctx, conn, http.MethodDelete, a.apiURL(conn, upstreamId), a.headers(conn), nil)
	return err
}

func geminiStatus(state string) string {
	switch state {
	case "BATCH_STATE_RUNNING":
		return "in_progress"
	case "BATCH_STATE_SUCCEEDED":
		return "completed"
	case "BATCH_STATE_FAILED":
		return "failed"
	case "BATCH_STATE_CANCELLED":
		return "cancelled"
	case "BATCH_STATE_EXPIRED":
		return "expired"
	default:
		return "validating"
	}
}

func parseGeminiOperation(data []byte) (*UpstreamBatch, error) {
	var op geminiOperation
	if err := common.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	if op.Name == "" {
		return nil, fmt.Errorf("gemini batch response has no name")
	}
	batch := &UpstreamBatch{Id: op.Name, Status: geminiStatus(op.Metadata.State)}
	batch.Total, _ = strconv.Atoi(op.Metadata.BatchStats.RequestCount)
	batch.Completed, _ = strconv.Atoi(op.Metadata.BatchStats.SuccessfulRequestCount)
	batch.Failed, _ = strconv.Atoi(op.Metadata.BatchStats.FailedRequestCount)
	if op.Error != nil {
		batch.ErrorMessage = op.Error.Message
	}
	output := op.Response
	if output == nil {
		output = op.Metadata.Output
	}
	if !isTerminalStatus(batch.Status) {
		return batch, nil
	}
	if output != nil && output.ResponsesFile != "" {
		return nil, fmt.Errorf("gemini batch %s returned a responses file, which inline batches do not expect", op.Name)
	}
	if output != nil && output.InlinedResponses != nil {
		batch.ResultsReady = true
		for index, item := range output.InlinedResponses.InlinedResponses {
			batch.Results = append(batch.Results, convertGeminiResult(index, item))
		}
		return batch, nil
	}
	// A succeeded batch always carries output; until it shows up the results
	// are not ready. Failed, cancelled and expired batches may have none.
	batch.ResultsReady = op.Done && batch.Status != "completed"
	return batch, nil
}

func convertGeminiResult(index int, item geminiInlinedResponse) UpstreamResult {
	result := UpstreamResult{CustomId: item.Metadata.Key, Index: index}
	if item.Error != nil {
		result.StatusCode = geminiErrorStatus(item.Error.Code)
		result.ErrorCode = strconv.Itoa(item.Error.Code)
		result.ErrorMessage = item.Error.Message
		if result.ErrorMessage == "" {
			result.ErrorMessage = "request failed"
		}
		return result
	}
	var response dto.GeminiChatResponse
	var extra struct {
		ModelVersion string `json:"modelVersion"`
		ResponseId   string `json:"responseId"`
	}
	if len(item.Response) == 0 || common.Unmarshal(item.Response, &response) != nil || common.Unmarshal(item.Response, &extra) != nil {
		result.StatusCode = http.StatusBadGateway
		result.ErrorCode = "invalid_upstream_response"
		result.ErrorMessage = "the upstream returned no readable response for this request"
		return result
	}
	result.StatusCode = http.StatusOK
	result.RequestId = extra.ResponseId
	result.Model = extra.ModelVersion
	if response.HasUsageMetadata {
		result.Usage = relayconvert.UsageFromGeminiMetadata(&response.UsageMetadata, 0)
	}
	openAIResponse := relayconvert.ResponseGeminiChat2OpenAI("chatcmpl-"+extra.ResponseId, time.Now().Unix(), &response)
	openAIResponse.Model = extra.ModelVersion
	if result.Usage != nil {
		openAIResponse.Usage = *result.Usage
	}
	body, err := common.Marshal(openAIResponse)
	if err != nil {
		result.StatusCode = http.StatusBadGateway
		result.ErrorCode = "invalid_upstream_response"
		result.ErrorMessage = err.Error()
		return result
	}
	result.Body = body
	return result
}

// geminiErrorStatus maps a google.rpc.Code to the HTTP status Google uses for
// it.
func geminiErrorStatus(code int) int {
	switch code {
	case 3, 9, 11:
		return http.StatusBadRequest
	case 4:
		return http.StatusGatewayTimeout
	case 5:
		return http.StatusNotFound
	case 7:
		return http.StatusForbidden
	case 8:
		return http.StatusTooManyRequests
	case 14:
		return http.StatusServiceUnavailable
	case 16:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}
