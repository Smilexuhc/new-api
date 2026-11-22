package batch

import (
	"encoding/json"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
)

// FileObject is the OpenAI File object.
type FileObject struct {
	Id        string `json:"id"`
	Object    string `json:"object"`
	Bytes     int64  `json:"bytes"`
	CreatedAt int64  `json:"created_at"`
	ExpiresAt *int64 `json:"expires_at"`
	Filename  string `json:"filename"`
	Purpose   string `json:"purpose"`
	Status    string `json:"status"`
}

type RequestCounts struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type BatchErrorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   any    `json:"param"`
	Line    *int   `json:"line"`
}

type BatchErrors struct {
	Object string           `json:"object"`
	Data   []BatchErrorData `json:"data"`
}

// BatchUsage reports the tokens the charge is based on and, as an extension,
// the upstream model versions that served the batch.
type BatchUsage struct {
	InputTokens         int            `json:"input_tokens"`
	OutputTokens        int            `json:"output_tokens"`
	TotalTokens         int            `json:"total_tokens"`
	InputTokensDetails  map[string]int `json:"input_tokens_details"`
	OutputTokensDetails map[string]int `json:"output_tokens_details"`
	ModelVersions       map[string]int `json:"model_versions,omitempty"`
	ResolvedModel       string         `json:"resolved_model,omitempty"`
}

// BatchObject is the OpenAI Batch object.
type BatchObject struct {
	Id               string            `json:"id"`
	Object           string            `json:"object"`
	Endpoint         string            `json:"endpoint"`
	Model            string            `json:"model"`
	Errors           *BatchErrors      `json:"errors"`
	InputFileId      string            `json:"input_file_id"`
	CompletionWindow string            `json:"completion_window"`
	Status           string            `json:"status"`
	OutputFileId     *string           `json:"output_file_id"`
	ErrorFileId      *string           `json:"error_file_id"`
	CreatedAt        int64             `json:"created_at"`
	InProgressAt     *int64            `json:"in_progress_at"`
	ExpiresAt        *int64            `json:"expires_at"`
	FinalizingAt     *int64            `json:"finalizing_at"`
	CompletedAt      *int64            `json:"completed_at"`
	FailedAt         *int64            `json:"failed_at"`
	ExpiredAt        *int64            `json:"expired_at"`
	CancellingAt     *int64            `json:"cancelling_at"`
	CancelledAt      *int64            `json:"cancelled_at"`
	RequestCounts    RequestCounts     `json:"request_counts"`
	Usage            *BatchUsage       `json:"usage"`
	Metadata         map[string]string `json:"metadata"`
}

type ListObject[T any] struct {
	Object  string  `json:"object"`
	Data    []T     `json:"data"`
	FirstId *string `json:"first_id"`
	LastId  *string `json:"last_id"`
	HasMore bool    `json:"has_more"`
}

// batchViewStatus reports a held batch as held. The stored status stays
// validating or in_progress so a resume continues from where the poll left
// off; clients that only watch the public status would otherwise wait forever.
func batchViewStatus(b *model.Batch) string {
	if b.BillingState == model.BatchBillingHeld {
		return model.BatchBillingHeld
	}
	return b.Status
}

func optionalTime(value int64) *int64 {
	if value == 0 {
		return nil
	}
	return &value
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func NewFileObject(file *model.BatchFile) FileObject {
	return FileObject{
		Id:        file.FileId,
		Object:    "file",
		Bytes:     file.Bytes,
		CreatedAt: file.CreatedAt,
		ExpiresAt: optionalTime(file.ExpiresAt),
		Filename:  file.Filename,
		Purpose:   file.Purpose,
		Status:    "processed",
	}
}

func NewBatchObject(b *model.Batch) BatchObject {
	object := BatchObject{
		Id:               b.BatchId,
		Object:           "batch",
		Endpoint:         b.Endpoint,
		Model:            b.Model,
		InputFileId:      b.InputFileId,
		CompletionWindow: b.CompletionWindow,
		Status:           batchViewStatus(b),
		OutputFileId:     optionalString(b.OutputFileId),
		ErrorFileId:      optionalString(b.ErrorFileId),
		CreatedAt:        b.CreatedAt,
		InProgressAt:     optionalTime(b.InProgressAt),
		ExpiresAt:        optionalTime(b.ExpiresAt),
		FinalizingAt:     optionalTime(b.FinalizingAt),
		CompletedAt:      optionalTime(b.CompletedAt),
		FailedAt:         optionalTime(b.FailedAt),
		ExpiredAt:        optionalTime(b.ExpiredAt),
		CancellingAt:     optionalTime(b.CancellingAt),
		CancelledAt:      optionalTime(b.CancelledAt),
		RequestCounts: RequestCounts{
			Total:     b.RequestTotal,
			Completed: b.RequestCompleted,
			Failed:    b.RequestFailed,
		},
	}
	if b.Errors != "" {
		var errs BatchErrors
		if common.Unmarshal([]byte(b.Errors), &errs) == nil {
			object.Errors = &errs
		}
	}
	if b.Metadata != "" {
		var metadata map[string]string
		if common.Unmarshal([]byte(b.Metadata), &metadata) == nil {
			object.Metadata = metadata
		}
	}
	if b.UsageSummary != "" {
		var summary usageSummary
		if common.Unmarshal([]byte(b.UsageSummary), &summary) == nil {
			object.Usage = summary.view()
		}
	}
	return object
}

func batchErrorsJSON(code string, message string) string {
	data, _ := common.Marshal(BatchErrors{Object: "list", Data: []BatchErrorData{{Code: code, Message: cleanMessage(message)}}})
	return string(data)
}

// outputLine is one line of an output or error file.
type outputLine struct {
	Id       string          `json:"id"`
	CustomId string          `json:"custom_id"`
	Response *outputResponse `json:"response"`
	Error    *outputError    `json:"error"`
}

type outputResponse struct {
	StatusCode int             `json:"status_code"`
	RequestId  string          `json:"request_id"`
	Body       json.RawMessage `json:"body"`
}

type outputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
