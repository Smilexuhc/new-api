package batch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/dto"
)

// openRouterAdaptor uses POST /api/v1/batches, which takes the requests inline
// and returns the results inline once the batch is terminal. OpenRouter bills
// batch requests at half the synchronous price and has no cancel operation.
type openRouterAdaptor struct{}

// openRouterCreateRequest field order matters: OpenRouter parses the body as
// a stream and requires requests to be the last field.
type openRouterCreateRequest struct {
	Endpoint         string              `json:"endpoint"`
	Model            string              `json:"model"`
	CompletionWindow string              `json:"completion_window,omitempty"`
	Requests         []openRouterRequest `json:"requests"`
}

type openRouterRequest struct {
	CustomId string          `json:"custom_id"`
	Body     json.RawMessage `json:"body"`
}

type openRouterBatch struct {
	Id            string `json:"id"`
	Status        string `json:"status"`
	Model         string `json:"model"`
	ExpiresAt     *int64 `json:"expires_at"`
	RequestCounts struct {
		Total     int `json:"total"`
		Completed int `json:"completed"`
		Failed    int `json:"failed"`
	} `json:"request_counts"`
	Usage *struct {
		Cost *float64 `json:"cost"`
	} `json:"usage"`
	Results []openRouterResult `json:"results"`
	Error   json.RawMessage    `json:"error"`
}

type openRouterResult struct {
	CustomId string `json:"custom_id"`
	Response *struct {
		StatusCode int             `json:"status_code"`
		RequestId  string          `json:"request_id"`
		Body       json.RawMessage `json:"body"`
	} `json:"response"`
	Error *struct {
		Code    any    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (a *openRouterAdaptor) DefaultPriceRatio() float64 {
	return 0.5
}

func (a *openRouterAdaptor) SupportsEndpoint(endpoint string) bool {
	return endpoint == EndpointChatCompletions
}

func (a *openRouterAdaptor) headers(conn *Connection) map[string]string {
	return map[string]string{"Authorization": "Bearer " + conn.Key}
}

func (a *openRouterAdaptor) batchesURL(conn *Connection) string {
	return strings.TrimRight(conn.BaseURL, "/") + "/v1/batches"
}

func buildOpenRouterCreateBody(req *SubmitRequest) ([]byte, error) {
	body := openRouterCreateRequest{
		Endpoint:         req.Endpoint,
		Model:            req.UpstreamModel,
		CompletionWindow: req.CompletionWindow,
		Requests:         make([]openRouterRequest, 0, len(req.Items)),
	}
	for _, item := range req.Items {
		body.Requests = append(body.Requests, openRouterRequest{CustomId: item.CustomId, Body: item.Body})
	}
	return common.Marshal(body)
}

func (a *openRouterAdaptor) Submit(ctx context.Context, conn *Connection, req *SubmitRequest) (*UpstreamBatch, error) {
	body, err := buildOpenRouterCreateBody(req)
	if err != nil {
		return nil, err
	}
	data, err := doUpstream(ctx, conn, http.MethodPost, a.batchesURL(conn), a.headers(conn), body)
	if err != nil {
		return nil, err
	}
	return parseOpenRouterBatch(data)
}

func (a *openRouterAdaptor) Retrieve(ctx context.Context, conn *Connection, upstreamId string) (*UpstreamBatch, error) {
	data, err := doUpstream(ctx, conn, http.MethodGet, a.batchesURL(conn)+"/"+url.PathEscape(upstreamId), a.headers(conn), nil)
	if err != nil {
		return nil, err
	}
	return parseOpenRouterBatch(data)
}

func (a *openRouterAdaptor) Cancel(ctx context.Context, conn *Connection, upstreamId string) (*UpstreamBatch, error) {
	return nil, ErrCancelNotSupported
}

func (a *openRouterAdaptor) Delete(ctx context.Context, conn *Connection, upstreamId string) error {
	_, err := doUpstream(ctx, conn, http.MethodDelete, a.batchesURL(conn)+"/"+url.PathEscape(upstreamId), a.headers(conn), nil)
	return err
}

func parseOpenRouterBatch(data []byte) (*UpstreamBatch, error) {
	var parsed openRouterBatch
	if err := common.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	batch := &UpstreamBatch{
		Id:        parsed.Id,
		Status:    parsed.Status,
		Total:     parsed.RequestCounts.Total,
		Completed: parsed.RequestCounts.Completed,
		Failed:    parsed.RequestCounts.Failed,
		// Results carry the requested alias; the batch names the dated version.
		ResolvedModel: parsed.Model,
	}
	if parsed.ExpiresAt != nil {
		batch.ExpiresAt = *parsed.ExpiresAt
	}
	if parsed.Usage != nil {
		batch.Cost = parsed.Usage.Cost
	}
	if len(parsed.Error) > 0 && string(parsed.Error) != "null" {
		batch.ErrorMessage = upstreamErrorMessage([]byte(`{"error":` + string(parsed.Error) + `}`))
	}
	if !isTerminalStatus(batch.Status) {
		return batch, nil
	}
	// results stays null for a short while after a batch turns terminal, and
	// for good when the batch ended before any request completed, such as a
	// batch rejected at validation.
	switch {
	case parsed.Results != nil:
		batch.ResultsReady = true
		batch.Results = make([]UpstreamResult, 0, len(parsed.Results))
		for _, item := range parsed.Results {
			batch.Results = append(batch.Results, convertOpenRouterResult(item))
		}
	case batch.Status != model.BatchStatusCompleted && batch.Completed == 0:
		batch.ResultsReady = true
	}
	return batch, nil
}

func convertOpenRouterResult(item openRouterResult) UpstreamResult {
	result := UpstreamResult{CustomId: item.CustomId, Index: -1}
	if item.Response != nil {
		result.StatusCode = item.Response.StatusCode
		result.RequestId = item.Response.RequestId
		result.Body = item.Response.Body
		var body struct {
			Model string     `json:"model"`
			Usage *dto.Usage `json:"usage"`
		}
		if len(item.Response.Body) > 0 && common.Unmarshal(item.Response.Body, &body) == nil {
			result.Model = body.Model
			result.Usage = body.Usage
			if body.Usage != nil {
				result.Cost = usageCost(body.Usage.Cost)
			}
		}
	}
	if item.Error != nil {
		result.ErrorMessage = item.Error.Message
		if item.Error.Code != nil {
			result.ErrorCode = common.Interface2String(item.Error.Code)
		}
		if result.ErrorMessage == "" {
			result.ErrorMessage = "request failed"
		}
	}
	return result
}

func usageCost(cost any) *float64 {
	switch value := cost.(type) {
	case float64:
		return &value
	case json.Number:
		if parsed, err := value.Float64(); err == nil {
			return &parsed
		}
	}
	return nil
}
