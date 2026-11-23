// Package batch forwards OpenAI-compatible batches to upstream native batch
// APIs and settles their charge from the usage the upstream reports.
package batch

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/gin-gonic/gin"
)

var ErrCancelNotSupported = errors.New("the upstream batch API has no cancel operation")

// ErrNoUpstreamId is returned when the upstream accepted a submission without
// naming the batch; it may run upstream and cannot be tracked.
var ErrNoUpstreamId = errors.New("the upstream accepted the batch without returning its id")

// Connection addresses one upstream account: a channel and one of its keys.
type Connection struct {
	ChannelType int
	BaseURL     string
	Key         string
	Proxy       string
}

// SubmitItem is one validated input line.
type SubmitItem struct {
	CustomId string
	// Body is the request body in the upstream format, with the upstream
	// model name.
	Body []byte
}

type SubmitRequest struct {
	BatchId          string
	Endpoint         string
	UpstreamModel    string
	CompletionWindow string
	Items            []SubmitItem
}

// UpstreamBatch is the state of an upstream batch mapped to OpenAI names.
type UpstreamBatch struct {
	Id        string
	Status    string
	Total     int
	Completed int
	Failed    int
	ExpiresAt int64
	// ResultsReady is true once Results holds every result the upstream will
	// return. Only terminal batches have results.
	ResultsReady bool
	// ResultsLost is true when the upstream finished the batch, reports
	// succeeded requests and will never return their results.
	ResultsLost bool
	Results     []UpstreamResult
	// Cost is the upstream charge in USD when the upstream reports one.
	Cost         *float64
	ErrorMessage string
	// ResolvedModel is the dated model version the upstream resolved for the
	// whole batch, when it reports one apart from the per-result model.
	ResolvedModel string
}

// UpstreamResult is the result of one request converted to an OpenAI batch
// output line.
type UpstreamResult struct {
	CustomId string
	// Index is the input position when the upstream does not echo custom_id,
	// otherwise -1.
	Index      int
	StatusCode int
	RequestId  string
	// Body is the OpenAI response body. Its model field holds the model
	// version the upstream reports for this request.
	Body  []byte
	Usage *dto.Usage
	Model string
	// Cost is the upstream charge in USD for this request when reported.
	Cost *float64
	// GroundingQueries is the number of distinct search queries a grounded
	// response ran; FunctionCalls names the function calls it made. Both are
	// priced by the tool price table like a synchronous response.
	GroundingQueries int
	FunctionCalls    []string
	ErrorCode        string
	ErrorMessage     string
}

func (r *UpstreamResult) Succeeded() bool {
	return r.ErrorMessage == "" && r.ErrorCode == "" && r.StatusCode >= 200 && r.StatusCode < 300
}

// UpstreamError is an HTTP error answered by the upstream.
type UpstreamError struct {
	StatusCode int
	Message    string
	// KeyRejected is set when the upstream refused the channel's key: a 401,
	// or the "API key not valid" answer some upstreams give with another code.
	KeyRejected bool
}

// channelFault reports whether the error is about the channel's credentials
// or permissions rather than the request.
func (e *UpstreamError) channelFault() bool {
	return e.KeyRejected || e.StatusCode == 401 || e.StatusCode == 403
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("upstream status %d: %s", e.StatusCode, e.Message)
}

// IsUpstreamNotFound reports whether the upstream no longer knows the batch.
func IsUpstreamNotFound(err error) bool {
	var upstreamErr *UpstreamError
	return errors.As(err, &upstreamErr) && (upstreamErr.StatusCode == 404 || upstreamErr.StatusCode == 410)
}

// RequestConverter is implemented by adaptors whose upstream takes another
// request format. It runs while the batch is created, inside the client
// request, with the relay info of the selected channel.
type RequestConverter interface {
	ConvertRequest(c *gin.Context, info *relaycommon.RelayInfo, request *dto.GeneralOpenAIRequest) ([]byte, error)
}

// Adaptor talks to one upstream native batch API.
type Adaptor interface {
	// DefaultPriceRatio is the price of a batch request relative to the same
	// synchronous request on this upstream.
	DefaultPriceRatio() float64
	SupportsEndpoint(endpoint string) bool
	Submit(ctx context.Context, conn *Connection, req *SubmitRequest) (*UpstreamBatch, error)
	Retrieve(ctx context.Context, conn *Connection, upstreamId string) (*UpstreamBatch, error)
	SupportsCancel() bool
	// Cancel returns ErrCancelNotSupported when the upstream cannot cancel.
	Cancel(ctx context.Context, conn *Connection, upstreamId string) (*UpstreamBatch, error)
	// Delete removes a terminal batch from the upstream.
	Delete(ctx context.Context, conn *Connection, upstreamId string) error
}

var adaptors = map[int]Adaptor{
	constant.ChannelTypeOpenRouter: &openRouterAdaptor{},
	constant.ChannelTypeGemini:     &geminiAdaptor{},
}

func GetAdaptor(channelType int) Adaptor {
	return adaptors[channelType]
}

// SupportedChannelTypes lists the channel types a batch can be submitted to.
func SupportedChannelTypes() []int {
	types := make([]int, 0, len(adaptors))
	for channelType := range adaptors {
		types = append(types, channelType)
	}
	slices.Sort(types)
	return types
}
