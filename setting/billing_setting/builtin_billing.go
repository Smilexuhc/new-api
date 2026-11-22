package billing_setting

import (
	"time"

	"github.com/QuantumNous/new-api/constant"
)

// deepSeekPeak is DeepSeek's peak window: Monday to Friday, 09:00-12:00 and
// 14:00-18:00 Beijing time. The Chinese public holidays DeepSeek bills
// off-peak are not known in advance and bill at peak here.
const deepSeekPeak = `weekday("Asia/Shanghai") >= 1 && weekday("Asia/Shanghai") <= 5 && ((hour("Asia/Shanghai") >= 9 && hour("Asia/Shanghai") < 12) || (hour("Asia/Shanghai") >= 14 && hour("Asia/Shanghai") < 18))`

const deepSeekFlashExpr = deepSeekPeak + ` ? tier("peak", p * 0.3 + cr * 0.006 + c * 1.2) : tier("off_peak", p * 0.15 + cr * 0.003 + c * 0.6)`

// Built-in token prices use actual USD per million tokens. Keep new model
// defaults here instead of splitting them across the legacy ratio tables.
var builtinBillingExpr = map[string]string{
	// https://developers.openai.com/api/docs/pricing (Standard, 2026-09-09).
	// The Images API reports image output in output_tokens, normalized to c.
	"gpt-image-2":            `tier("standard", p * 5 + cr * 1.25 + img * 8 + img_cr * 2 + c * 30)`,
	"gpt-image-2.5-sunburst": `tier("standard", p * 5 + cr * 1.25 + img * 8 + img_cr * 2 + c * 30)`,
	"gpt-image-2.5-flare":    `tier("standard", p * 5 + cr * 1.25 + img * 8 + img_cr * 2 + c * 30)`,
	// https://developers.openai.com/api/docs/models/gpt-6-astra
	// https://developers.openai.com/api/docs/pricing (Standard, 2026-10-22):
	// cc is the 30m cache write and cc12h the 12h write. The long-context
	// rates apply to the whole request.
	// Do not infer service-tier discounts from incoming request parameters:
	// channels filter service_tier by default, so it may not reach the upstream.
	"gpt-6-astra": `len <= 272000 ? tier("standard", p * 10 + c * 50 + cr * 1 + cc * 12.5 + cc12h * 25) : tier("long_context", p * 20 + c * 75 + cr * 2 + cc * 25 + cc12h * 50)`,
	// https://api-docs.deepseek.com/quick_start/pricing (2026-10-17). Off-peak
	// is half the peak price. deepseek-v4-flash and deepseek-v4-flash-vision-exp
	// run on V4.1 Flash and bill at the Flash price. Images bill as input
	// tokens, and there is no cache-write price.
	"deepseek-flash":               deepSeekFlashExpr,
	"deepseek-v4-flash":            deepSeekFlashExpr,
	"deepseek-v4-flash-vision-exp": deepSeekFlashExpr,
	"deepseek-v4-pro":              deepSeekPeak + ` ? tier("peak", p * 1.32 + cr * 0.044 + c * 3.96) : tier("off_peak", p * 0.66 + cr * 0.022 + c * 1.98)`,
	// The Search and Fetch API routing names: $0.75 per 1,000 successful
	// requests each, the same pricing page. The calls carry no tokens.
	constant.DeepSeekSearchModel: `tier("request", fixed(0.00075))`,
	constant.DeepSeekFetchModel:  `tier("request", fixed(0.00075))`,
	// https://ai.google.dev/gemini-api/docs/pricing (Standard, 2026-11-22).
	// Output prices include thinking tokens, which Gemini usage already counts
	// as completion. Audio input costs the text price unless listed. Cached
	// audio of 3.1 Flash-Lite ($0.05) bills at the text cache price, since cr
	// does not split by modality.
	"gemini-4-neon":         `tier("standard", p * 3 + cr * 0.15 + c * 15)`,
	"gemini-4-hydrogen":     `tier("standard", p * 1.5 + cr * 0.075 + c * 7.5)`,
	"gemini-3.1-flash-lite": `tier("standard", p * 0.25 + ai * 0.5 + cr * 0.025 + c * 1.5)`,
}

// scheduledBillingExpr is a built-in price that takes effect at From.
type scheduledBillingExpr struct {
	From time.Time
	Expr string
}

// Google publishes price changes by calendar date without a time zone; they
// take effect here at midnight Pacific time.
var (
	gemini4PriceChange    = time.Date(2026, time.December, 1, 8, 0, 0, 0, time.UTC)
	gemini38PriceChange   = time.Date(2027, time.January, 1, 8, 0, 0, 0, time.UTC)
	gemini4CarbonSchedule = []scheduledBillingExpr{
		{Expr: `tier("standard", p * 2 + cr * 0.1 + c * 10)`},
		{From: gemini4PriceChange, Expr: `tier("standard", p * 4 + cr * 0.2 + c * 20)`},
	}
)

// builtinBillingSchedule holds built-in prices with a published change date,
// oldest first; the first entry has no start. A model listed here must not
// also be in builtinBillingExpr. Same source as the Gemini prices above.
var builtinBillingSchedule = map[string][]scheduledBillingExpr{
	"gemini-4-carbon": gemini4CarbonSchedule,
	"gemini-4-argon":  gemini4CarbonSchedule,
	"gemini-3.8-flash": {
		{Expr: `tier("standard", p * 0.75 + cr * 0.075 + c * 3.75)`},
		{From: gemini38PriceChange, Expr: `tier("standard", p * 1.5 + cr * 0.15 + c * 7.5)`},
	},
}

// builtinBillingExprAt returns the built-in expression of model in effect at
// now. A request freezes the expression in its billing snapshot, so a price
// change does not reprice requests already reserved.
func builtinBillingExprAt(model string, now time.Time) (string, bool) {
	if expr, ok := builtinBillingExpr[model]; ok {
		return expr, true
	}
	entries, ok := builtinBillingSchedule[model]
	if !ok || len(entries) == 0 {
		return "", false
	}
	expr := entries[0].Expr
	for _, entry := range entries[1:] {
		if !now.Before(entry.From) {
			expr = entry.Expr
		}
	}
	return expr, true
}

func builtinBillingModels() []string {
	models := make([]string, 0, len(builtinBillingExpr)+len(builtinBillingSchedule))
	for model := range builtinBillingExpr {
		models = append(models, model)
	}
	for model := range builtinBillingSchedule {
		models = append(models, model)
	}
	return models
}
