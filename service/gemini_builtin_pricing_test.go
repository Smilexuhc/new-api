package service

import (
	"fmt"
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/pkg/billingexpr"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/billing_setting"
	hosttypes "github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// geminiOfficialPrice is a Standard price from
// https://ai.google.dev/gemini-api/docs/pricing in USD per 1M tokens. Audio
// is zero when audio input costs the text price.
type geminiOfficialPrice struct {
	input, audio, output, cache float64
}

// geminiOfficialPriceAt restates the published prices independently of the
// built-in expressions, including the announced price changes.
func geminiOfficialPriceAt(model string, now time.Time) geminiOfficialPrice {
	gemini4Change := time.Date(2026, time.December, 1, 8, 0, 0, 0, time.UTC)
	gemini38Change := time.Date(2027, time.January, 1, 8, 0, 0, 0, time.UTC)
	switch model {
	case "gemini-4-carbon", "gemini-4-argon":
		if now.Before(gemini4Change) {
			return geminiOfficialPrice{input: 2, output: 10, cache: 0.1}
		}
		return geminiOfficialPrice{input: 4, output: 20, cache: 0.2}
	case "gemini-4-neon":
		return geminiOfficialPrice{input: 3, output: 15, cache: 0.15}
	case "gemini-4-hydrogen":
		return geminiOfficialPrice{input: 1.5, output: 7.5, cache: 0.075}
	case "gemini-3.8-flash":
		if now.Before(gemini38Change) {
			return geminiOfficialPrice{input: 0.75, output: 3.75, cache: 0.075}
		}
		return geminiOfficialPrice{input: 1.5, output: 7.5, cache: 0.15}
	case "gemini-3.1-flash-lite":
		return geminiOfficialPrice{input: 0.25, audio: 0.5, output: 1.5, cache: 0.025}
	}
	panic("no official price for " + model)
}

type geminiUsageCase struct {
	name                       string
	text, audio, image, cached int
	candidates, thoughts       int
}

// cost restates the bill by hand: cached tokens are text, thinking tokens
// are output, and audio has its own price when the model lists one.
func (u geminiUsageCase) cost(price geminiOfficialPrice) float64 {
	prompt := u.text + u.audio + u.image
	uncached := float64(prompt - u.cached)
	audio := 0.0
	if price.audio > 0 {
		uncached -= float64(u.audio)
		audio = float64(u.audio) * price.audio
	}
	return (uncached*price.input + audio + float64(u.cached)*price.cache +
		float64(u.candidates+u.thoughts)*price.output) / 1_000_000
}

func (u geminiUsageCase) usage() *dto.Usage {
	metadata := &dto.GeminiUsageMetadata{
		PromptTokenCount:        u.text + u.audio + u.image,
		CandidatesTokenCount:    u.candidates,
		ThoughtsTokenCount:      u.thoughts,
		CachedContentTokenCount: u.cached,
		TotalTokenCount:         u.text + u.audio + u.image + u.candidates + u.thoughts,
	}
	for _, detail := range []dto.GeminiPromptTokensDetails{
		{Modality: "TEXT", TokenCount: u.text},
		{Modality: "AUDIO", TokenCount: u.audio},
		{Modality: "IMAGE", TokenCount: u.image},
	} {
		if detail.TokenCount > 0 {
			metadata.PromptTokensDetails = append(metadata.PromptTokensDetails, detail)
		}
	}
	return relayconvert.UsageFromGeminiMetadata(metadata, 0)
}

var geminiUsageCases = []geminiUsageCase{
	{name: "text", text: 1200, candidates: 300},
	{name: "text with thinking", text: 1200, candidates: 300, thoughts: 700},
	{name: "audio caption", text: 900, audio: 4800, candidates: 400},
	{name: "audio caption with thinking", text: 900, audio: 4800, candidates: 400, thoughts: 1100},
	{name: "mixed with cache and thinking", text: 2000, audio: 1500, image: 516, cached: 1024, candidates: 250, thoughts: 500},
}

var geminiBuiltinModels = []string{
	"gemini-4-carbon", "gemini-4-argon", "gemini-4-neon", "gemini-4-hydrogen",
	"gemini-3.8-flash", "gemini-3.1-flash-lite",
}

func geminiBuiltinRelayInfo(t *testing.T, model string, groupRatio float64) *relaycommon.RelayInfo {
	t.Helper()
	require.Equal(t, billing_setting.BillingModeTieredExpr, billing_setting.GetBillingMode(model))
	expr, ok := billing_setting.GetBillingExpr(model)
	require.True(t, ok)
	return &relaycommon.RelayInfo{
		UserId:          71,
		OriginModelName: model,
		UsingGroup:      "default",
		StartTime:       time.Now(),
		RelayFormat:     types.RelayFormatOpenAI,
		ChannelMeta:     &relaycommon.ChannelMeta{ChannelId: 71, UpstreamModelName: model},
		PriceData:       hosttypes.PriceData{GroupRatioInfo: hosttypes.GroupRatioInfo{GroupRatio: groupRatio}},
		TieredBillingSnapshot: &billingexpr.BillingSnapshot{
			BillingMode:  billing_setting.BillingModeTieredExpr,
			ModelName:    model,
			ExprString:   expr,
			ExprHash:     billingexpr.ExprHashString(expr),
			GroupRatio:   groupRatio,
			QuotaPerUnit: testQuotaPerUnit,
		},
	}
}

// TestGeminiBuiltinPricesMatchOfficialPrices settles every model, modality and
// thinking mix through the synchronous consume path and the batch pricing
// path, and compares both with the published price computed by hand.
func TestGeminiBuiltinPricesMatchOfficialPrices(t *testing.T) {
	truncate(t)
	seedUser(t, 71, 1_000_000_000)
	seedChannel(t, 71)
	gin.SetMode(gin.TestMode)

	for _, model := range geminiBuiltinModels {
		price := geminiOfficialPriceAt(model, time.Now())
		for _, usageCase := range geminiUsageCases {
			for _, groupRatio := range []float64{1, 1.5} {
				name := fmt.Sprintf("%s/%s/group %g", model, usageCase.name, groupRatio)
				want := usageCase.cost(price) * testQuotaPerUnit * groupRatio
				t.Run(name+"/sync", func(t *testing.T) {
					ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
					ctx.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
					PostTextConsumeQuota(ctx, geminiBuiltinRelayInfo(t, model, groupRatio), usageCase.usage(), nil)
					log := getLastLog(t)
					require.NotNil(t, log)
					assert.Equal(t, int(math.Round(want)), log.Quota)
				})
				t.Run(name+"/batch", func(t *testing.T) {
					ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
					info := geminiBuiltinRelayInfo(t, model, groupRatio)
					info.PriceData.AddOtherRatio("batch_ratio", 0.5)
					got, err := PriceTextUsage(ctx, info, usageCase.usage())
					require.NoError(t, err)
					assert.Equal(t, int(math.Round(want*0.5)), got.Quota)
				})
			}
		}
	}
}

// TestPriceTextUsageKeepsRatioPricingWithoutExpression guards the ratio path
// that models without an expression still take in a batch.
func TestPriceTextUsageKeepsRatioPricingWithoutExpression(t *testing.T) {
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	info := &relaycommon.RelayInfo{
		OriginModelName: "ratio-model",
		StartTime:       time.Now(),
		RelayFormat:     types.RelayFormatOpenAI,
		ChannelMeta:     &relaycommon.ChannelMeta{},
		PriceData: hosttypes.PriceData{
			ModelRatio: 1.25, CompletionRatio: 8,
			GroupRatioInfo: hosttypes.GroupRatioInfo{GroupRatio: 1},
		},
	}
	info.PriceData.AddOtherRatio("batch_ratio", 0.5)
	got, err := PriceTextUsage(ctx, info, &dto.Usage{PromptTokens: 1000, CompletionTokens: 100, TotalTokens: 1100})
	require.NoError(t, err)
	// (1000 + 100 * 8) * 1.25 * 0.5
	assert.Equal(t, 1125, got.Quota)
}

func TestPriceTextUsageReportsExpressionFailure(t *testing.T) {
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	info := geminiBuiltinRelayInfo(t, "gemini-4-hydrogen", 1)
	info.TieredBillingSnapshot.ExprString = `tier("standard", p * )`
	info.TieredBillingSnapshot.ExprHash = billingexpr.ExprHashString(info.TieredBillingSnapshot.ExprString)
	_, err := PriceTextUsage(ctx, info, &dto.Usage{PromptTokens: 10, TotalTokens: 10})
	require.Error(t, err)
}
