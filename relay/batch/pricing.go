package batch

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	hosttypes "github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
)

const batchRatioKey = "batch_ratio"

const (
	BatchRatioSourceSetting  = "setting"
	BatchRatioSourceUpstream = "upstream_default"
)

var dataURIPattern = regexp.MustCompile(`data:([A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+);base64,([A-Za-z0-9+/=]+)`)

// Media carried in a body is base64, far larger than its token count, so its
// decoded size estimates the tokens. Media given only as a link is not
// fetched and gets a fixed allowance. All rates err high for typical media;
// settlement charges what the upstream reports and refunds the rest.
const (
	textBytesPerToken = 3

	imageBaseTokens    = 258
	imageBytesPerToken = 250
	// Audio costs 32 tokens per second. 16 kHz 16-bit PCM is 32 KB per
	// second; compressed audio goes down to about 3 KB per second.
	pcmAudioBytesPerToken        = 500
	compressedAudioBytesPerToken = 90
	// Video costs up to about 300 tokens per second (a frame per second plus
	// its audio); 100 bytes per token covers streams down to 240 kbps.
	videoBytesPerToken = 100
	// A document page costs up to about 1,500 tokens.
	documentBytesPerToken = 20

	// A linked image costs at most about 1,600 tokens at default resolution.
	remoteImageTokens = 1600
	// A linked video or audio is assumed to last up to ten minutes; a longer
	// one is charged in full at settlement.
	remoteVideoTokens    = 600 * 300
	remoteAudioTokens    = 600 * 32
	remoteDocumentTokens = 20 * 1500
	// A link that is about to be downloaded and priced from its bytes counts
	// as one small image until then.
	prefetchLinkTokens = imageBaseTokens
)

// billingSnapshot freezes the price of a batch when it is created, so a price
// change while the batch runs does not change its charge. Tiered holds the
// expression of a model billed by tiered_expr; PriceData then carries only the
// group ratio.
type billingSnapshot struct {
	PriceData              hosttypes.PriceData          `json:"price_data"`
	Tiered                 *billingexpr.BillingSnapshot `json:"tiered,omitempty"`
	BatchRatio             float64                      `json:"batch_ratio"`
	BatchRatioSource       string                       `json:"batch_ratio_source"`
	OriginModel            string                       `json:"origin_model"`
	BillingModel           string                       `json:"billing_model"`
	UpstreamModel          string                       `json:"upstream_model"`
	ChannelType            int                          `json:"channel_type"`
	DefaultMaxOutputTokens int                          `json:"default_max_output_tokens"`
}

// relayInfo rebuilds what the text pricing reads from a request, with the
// batch ratio as one of the other ratios so it scales every price component.
func (s *billingSnapshot) relayInfo() *relaycommon.RelayInfo {
	info := &relaycommon.RelayInfo{
		OriginModelName:       s.OriginModel,
		BillingModelName:      s.BillingModel,
		StartTime:             time.Now(),
		RelayFormat:           types.RelayFormatOpenAI,
		TieredBillingSnapshot: s.Tiered,
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       s.ChannelType,
			UpstreamModelName: s.UpstreamModel,
		},
	}
	info.PriceData = s.PriceData
	info.PriceData.AddOtherRatio(batchRatioKey, s.BatchRatio)
	return info
}

// groundingReserveQueries is the number of search queries reserved for a
// request that may ground on Google Search; settlement charges the queries it
// ran.
const groundingReserveQueries = 10

// toolUsage is what one request ran that the tool price table bills.
type toolUsage struct {
	GroundingQueries int
	FunctionCalls    []string
}

// priceItem prices one request of the batch. An expression reads request
// parameters from the request's own body, not from the batch create call.
// Tool calls are priced like the synchronous relay prices them.
func priceItem(c *gin.Context, info *relaycommon.RelayInfo, line *inputLine, usage *dto.Usage, tools toolUsage) (service.TextUsagePrice, error) {
	info.BillingRequestInput = nil
	if info.TieredBillingSnapshot != nil && line != nil {
		info.BillingRequestInput = &billingexpr.RequestInput{Body: line.RawBody}
	}
	info.ResponsesUsageInfo = nil
	if tools.GroundingQueries > 0 {
		info.SetBillableToolCount(dto.BuildInToolGoogleSearch, tools.GroundingQueries)
		info.SetBillableToolCount(relaycommon.GoogleSearchGroundedPromptTool, 1)
	}
	for _, name := range tools.FunctionCalls {
		info.CountBillableToolCall(dto.BuildInCallFunctionCall, name)
	}
	return service.PriceTextUsage(c, info, usage)
}

// estimateUsage is the usage a request is expected to stay within: the input
// estimated from body, which is the request in OpenAI chat or Gemini format,
// and maxOutput, the output limit sent upstream.
func estimateUsage(body []byte, maxOutput int) *dto.Usage {
	return estimateUsageOf(body, maxOutput, false)
}

// estimateUsageOf is estimateUsage; with prefetch, image and video links are
// counted at prefetchLinkTokens because the request is priced again once
// they are downloaded.
func estimateUsageOf(body []byte, maxOutput int, prefetch bool) *dto.Usage {
	estimate := mediaEstimate{prefetch: prefetch}
	var root any
	if err := common.Unmarshal(body, &root); err == nil {
		estimate.walk(root)
	}
	textTokens := (max(len(body)-estimate.payloadBytes, 0) + textBytesPerToken - 1) / textBytesPerToken
	usage := &dto.Usage{
		PromptTokens:     textTokens + estimate.audioTokens + estimate.visualTokens,
		CompletionTokens: maxOutput,
	}
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	usage.PromptTokensDetails.AudioTokens = estimate.audioTokens
	// Images, video and documents bill at the text input price.
	usage.PromptTokensDetails.TextTokens = textTokens + estimate.visualTokens
	return usage
}

// mediaEstimate sums the tokens of the media in a request. payloadBytes is
// the base64 it found, which is not counted as text.
type mediaEstimate struct {
	payloadBytes int
	audioTokens  int
	visualTokens int
	remoteItems  int
	prefetch     bool
}

// remoteMediaCount is the number of media a request links to rather than
// carries.
func remoteMediaCount(body []byte) int {
	var estimate mediaEstimate
	var root any
	if err := common.Unmarshal(body, &root); err == nil {
		estimate.walk(root)
	}
	return estimate.remoteItems
}

func (e *mediaEstimate) walk(value any) {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			e.walk(item)
		}
	case map[string]any:
		for key, child := range v {
			if !e.field(key, child) {
				e.walk(child)
			}
		}
	case string:
		e.dataURIs(v)
	}
}

// field estimates a media field of an OpenAI chat or Gemini request and
// reports whether it did. Data URIs are left to walk, which finds them in any
// string, markdown images included.
func (e *mediaEstimate) field(key string, value any) bool {
	switch key {
	case "image_url", "video_url":
		url := mediaURL(value)
		if url == "" || strings.HasPrefix(url, "data:") {
			return false
		}
		e.remoteItems++
		if e.prefetch {
			e.visualTokens += prefetchLinkTokens
		} else if key == "image_url" {
			e.visualTokens += remoteImageTokens
		} else {
			e.visualTokens += remoteVideoTokens
		}
		return true
	case "input_audio":
		audio, ok := value.(map[string]any)
		data, _ := audio["data"].(string)
		if !ok || data == "" || strings.HasPrefix(data, "data:") {
			return false
		}
		format, _ := audio["format"].(string)
		e.inline("audio/"+format, data)
		return true
	case "file":
		file, ok := value.(map[string]any)
		if !ok {
			return false
		}
		data, _ := file["file_data"].(string)
		id, _ := file["file_id"].(string)
		if strings.HasPrefix(data, "data:") || (data == "" && id == "") {
			return false
		}
		e.remoteItems++
		e.visualTokens += remoteDocumentTokens
		return true
	case "inlineData", "inline_data":
		inline, ok := value.(map[string]any)
		data, _ := inline["data"].(string)
		if !ok || data == "" {
			return false
		}
		e.inline(firstString(inline, "mimeType", "mime_type"), data)
		return true
	case "fileData", "file_data":
		// Gemini fileData is an object; OpenAI file_data is a string.
		file, ok := value.(map[string]any)
		if !ok {
			return false
		}
		e.remote(firstString(file, "mimeType", "mime_type"))
		return true
	}
	return false
}

func (e *mediaEstimate) dataURIs(text string) {
	if !strings.Contains(text, "data:") {
		return
	}
	for _, match := range dataURIPattern.FindAllStringSubmatch(text, -1) {
		e.payloadBytes += len(match[0]) - len(match[2])
		e.inline(match[1], match[2])
	}
}

func (e *mediaEstimate) inline(mimeType string, base64Data string) {
	e.payloadBytes += len(base64Data)
	decoded := len(base64Data) * 3 / 4
	mimeType = strings.ToLower(mimeType)
	switch {
	case strings.HasPrefix(mimeType, "audio/"):
		if isPCMAudio(mimeType) {
			e.audioTokens += decoded/pcmAudioBytesPerToken + 1
		} else {
			e.audioTokens += decoded/compressedAudioBytesPerToken + 1
		}
	case strings.HasPrefix(mimeType, "video/"):
		e.visualTokens += videoTokens(base64Data)
	case strings.HasPrefix(mimeType, "image/"):
		// Gemini 3 bills an image by resolution, not size, so a small file
		// still costs about 1,100 tokens.
		e.visualTokens += max(remoteImageTokens, imageBaseTokens+decoded/imageBytesPerToken)
	default:
		e.visualTokens += imageBaseTokens + decoded/documentBytesPerToken
	}
}

func (e *mediaEstimate) remote(mimeType string) {
	e.remoteItems++
	mimeType = strings.ToLower(mimeType)
	switch {
	case strings.HasPrefix(mimeType, "audio/"):
		e.audioTokens += remoteAudioTokens
	case strings.HasPrefix(mimeType, "video/"):
		e.visualTokens += remoteVideoTokens
	case strings.HasPrefix(mimeType, "image/"):
		e.visualTokens += remoteImageTokens
	default:
		e.visualTokens += remoteDocumentTokens
	}
}

func isPCMAudio(mimeType string) bool {
	switch strings.TrimPrefix(mimeType, "audio/") {
	case "wav", "wave", "x-wav", "pcm", "l16", "aiff", "x-aiff":
		return true
	}
	return false
}

// mediaURL reads an OpenAI image_url or video_url, which is a string or an
// object with url.
func mediaURL(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case map[string]any:
		url, _ := v["url"].(string)
		return url
	}
	return ""
}

func firstString(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

type itemCharge struct {
	Quota            int
	PromptTokens     int
	CompletionTokens int
	Estimated        bool
}

// estimateCharges prices every line at its estimated usage. bodies holds the
// requests as sent upstream, in line order; without them a line is estimated
// from its uploaded body. Each charge saturates at the single-request bound,
// so the int64 total cannot wrap; the clamp, if any, belongs in the submit log.
func estimateCharges(c *gin.Context, snapshot *billingSnapshot, lines []*inputLine, bodies [][]byte) ([]int, int, *common.QuotaClamp, error) {
	return estimateChargesOf(c, snapshot, lines, bodies, false)
}

// estimateChargesOf is estimateCharges; prefetch is passed to estimateUsageOf.
func estimateChargesOf(c *gin.Context, snapshot *billingSnapshot, lines []*inputLine, bodies [][]byte, prefetch bool) ([]int, int, *common.QuotaClamp, error) {
	info := snapshot.relayInfo()
	charges := make([]int, len(lines))
	total := 0
	for index, line := range lines {
		body := line.RawBody
		if index < len(bodies) {
			body = bodies[index]
		}
		usage := estimateUsageOf(body, line.maxOutput(snapshot.DefaultMaxOutputTokens), prefetch)
		var tools toolUsage
		if snapshot.ChannelType == constant.ChannelTypeGemini && line.searchesWeb() {
			tools.GroundingQueries = groundingReserveQueries
		}
		price, err := priceItem(c, info, line, usage, tools)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("request %s: %w", line.CustomId, err)
		}
		charges[index] = price.Quota
		total += charges[index]
	}
	return charges, total, info.QuotaClamp, nil
}

// noteTieredExpr records the expression a batch is priced by, as the
// synchronous consume log does.
func noteTieredExpr(snap *billingexpr.BillingSnapshot, other *model.LogOther) {
	if snap == nil {
		return
	}
	other.SetPublic("billing_mode", "tiered_expr")
	other.SetPublic("expr_b64", base64.StdEncoding.EncodeToString([]byte(snap.ExprString)))
}

// noteQuotaSaturation records a quota clamp on a batch log so the anomaly is
// auditable by admins.
func noteQuotaSaturation(ctx context.Context, b *model.Batch, clamp *common.QuotaClamp, other *model.LogOther) {
	if clamp == nil {
		return
	}
	other.SetAdmin("quota_saturation", clamp.AuditMap())
	logger.LogWarn(ctx, common.LogText("quota saturation on batch %s: op=%s kind=%s original=%g clamped=%d user=%d model=%s",
		b.BatchId, clamp.Op, clamp.Kind, clamp.Original, clamp.Clamped, b.UserId, b.Model))
}

// chargeResult prices one result. Failed requests are free. A succeeded
// request is charged its estimate when the upstream billed it without saying
// how much, reported no or negative tokens, or when its usage cannot be
// priced. line is nil for a result the input does not list.
func chargeResult(c *gin.Context, info *relaycommon.RelayInfo, line *inputLine, result *UpstreamResult, estimate int) itemCharge {
	if result == nil || !result.Succeeded() {
		return itemCharge{}
	}
	if !usable(result.Usage) {
		return itemCharge{Quota: estimate, Estimated: true}
	}
	price, err := priceItem(c, info, line, result.Usage, toolUsage{GroundingQueries: result.GroundingQueries, FunctionCalls: result.FunctionCalls})
	if err != nil {
		common.SysError(common.LogText("batch result %s: pricing failed, charging its estimate: %s", result.CustomId, err.Error()))
		return itemCharge{Quota: estimate, Estimated: true}
	}
	return itemCharge{Quota: price.Quota, PromptTokens: price.PromptTokens, CompletionTokens: price.CompletionTokens}
}

// usable reports whether a usage reports tokens and none negative.
func usable(usage *dto.Usage) bool {
	if usage == nil || usage.PromptTokens < 0 || usage.CompletionTokens < 0 || usage.TotalTokens < 0 {
		return false
	}
	return usage.PromptTokens+usage.CompletionTokens > 0
}
