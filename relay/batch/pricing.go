package batch

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
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

// Media in a request body is base64, which is far larger than its token
// count. Its decoded size estimates the tokens instead: about one token per
// 500 bytes of audio (Gemini counts 32 tokens per second, 16 kHz 16-bit WAV
// is 32 KB per second) and a base image cost plus one token per 250 bytes for
// images. Both err high; settlement refunds the difference.
var (
	dataURIPattern    = regexp.MustCompile(`data:([A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+);base64,([A-Za-z0-9+/=]+)`)
	inputAudioPattern = regexp.MustCompile(`"data"\s*:\s*"([A-Za-z0-9+/=]{256,})"`)
)

const (
	audioBytesPerToken = 500
	imageBytesPerToken = 250
	imageBaseTokens    = 258
	textBytesPerToken  = 3
)

// billingSnapshot freezes the price of a batch when it is created, so a price
// change while the batch runs does not change its charge.
type billingSnapshot struct {
	PriceData              hosttypes.PriceData `json:"price_data"`
	BatchRatio             float64             `json:"batch_ratio"`
	BatchRatioSource       string              `json:"batch_ratio_source"`
	OriginModel            string              `json:"origin_model"`
	BillingModel           string              `json:"billing_model"`
	UpstreamModel          string              `json:"upstream_model"`
	ChannelType            int                 `json:"channel_type"`
	DefaultMaxOutputTokens int                 `json:"default_max_output_tokens"`
}

// relayInfo rebuilds what the text pricing reads from a request, with the
// batch ratio as one of the other ratios so it scales every price component.
func (s *billingSnapshot) relayInfo() *relaycommon.RelayInfo {
	info := &relaycommon.RelayInfo{
		OriginModelName:  s.OriginModel,
		BillingModelName: s.BillingModel,
		StartTime:        time.Now(),
		RelayFormat:      types.RelayFormatOpenAI,
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       s.ChannelType,
			UpstreamModelName: s.UpstreamModel,
		},
	}
	info.PriceData = s.PriceData
	info.PriceData.AddOtherRatio(batchRatioKey, s.BatchRatio)
	return info
}

// estimateUsage is the most a request can use: its estimated input and its
// output limit.
func estimateUsage(line *inputLine, defaultMaxOutput int) *dto.Usage {
	body := string(line.RawBody)
	remaining := len(body)
	audioTokens := 0
	imageTokens := 0
	for _, match := range dataURIPattern.FindAllStringSubmatch(body, -1) {
		remaining -= len(match[0])
		decoded := len(match[2]) * 3 / 4
		if strings.HasPrefix(match[1], "audio/") {
			audioTokens += decoded/audioBytesPerToken + 1
		} else {
			imageTokens += imageBaseTokens + decoded/imageBytesPerToken
		}
	}
	for _, match := range inputAudioPattern.FindAllStringSubmatch(body, -1) {
		remaining -= len(match[0])
		audioTokens += (len(match[1])*3/4)/audioBytesPerToken + 1
	}
	textTokens := (max(remaining, 0) + textBytesPerToken - 1) / textBytesPerToken
	output := line.MaxOutputTokens
	if output <= 0 {
		output = defaultMaxOutput
	}
	usage := &dto.Usage{
		PromptTokens:     textTokens + audioTokens + imageTokens,
		CompletionTokens: output,
	}
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	usage.PromptTokensDetails.AudioTokens = audioTokens
	usage.PromptTokensDetails.TextTokens = textTokens + imageTokens
	return usage
}

type itemCharge struct {
	Quota            int
	PromptTokens     int
	CompletionTokens int
	Estimated        bool
}

// estimateCharges prices every line at its estimated maximum usage. Each
// charge saturates at the single-request bound, so the int64 total cannot wrap;
// the clamp, if any, belongs in the submit log.
func estimateCharges(c *gin.Context, snapshot *billingSnapshot, lines []*inputLine) ([]int, int, *common.QuotaClamp) {
	info := snapshot.relayInfo()
	charges := make([]int, len(lines))
	total := 0
	for index, line := range lines {
		charges[index] = service.PriceTextUsage(c, info, estimateUsage(line, snapshot.DefaultMaxOutputTokens)).Quota
		total += charges[index]
	}
	return charges, total, info.QuotaClamp
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
// request without usage is charged its estimate, since the upstream billed it
// but did not say how much.
func chargeResult(c *gin.Context, info *relaycommon.RelayInfo, result *UpstreamResult, estimate int) itemCharge {
	if result == nil || !result.Succeeded() {
		return itemCharge{}
	}
	if result.Usage == nil {
		return itemCharge{Quota: estimate, Estimated: true}
	}
	price := service.PriceTextUsage(c, info, result.Usage)
	return itemCharge{Quota: price.Quota, PromptTokens: price.PromptTokens, CompletionTokens: price.CompletionTokens}
}
