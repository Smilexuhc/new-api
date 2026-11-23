package billing_setting

import (
	"testing"
	"time"

	"github.com/QuantumNous/new-api/pkg/billingexpr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuiltinBillingScheduleSwitchesAtPacificMidnight(t *testing.T) {
	pacific := time.FixedZone("PST", -8*60*60)
	for _, tc := range []struct {
		model  string
		at     time.Time
		expect string
	}{
		{"gemini-4-carbon", time.Date(2026, time.November, 30, 23, 59, 59, 0, pacific), `tier("standard", p * 2 + cr * 0.1 + c * 10)`},
		{"gemini-4-carbon", time.Date(2026, time.December, 1, 0, 0, 0, 0, pacific), `tier("standard", p * 4 + cr * 0.2 + c * 20)`},
		{"gemini-4-argon", time.Date(2026, time.November, 22, 12, 0, 0, 0, pacific), `tier("standard", p * 2 + cr * 0.1 + c * 10)`},
		{"gemini-4-argon", time.Date(2027, time.June, 1, 0, 0, 0, 0, pacific), `tier("standard", p * 4 + cr * 0.2 + c * 20)`},
		{"gemini-3.8-flash", time.Date(2026, time.December, 31, 23, 59, 59, 0, pacific), `tier("standard", p * 0.75 + cr * 0.075 + c * 3.75)`},
		{"gemini-3.8-flash", time.Date(2027, time.January, 1, 0, 0, 0, 0, pacific), `tier("standard", p * 1.5 + cr * 0.15 + c * 7.5)`},
		{"gemini-4-hydrogen", time.Date(2030, time.January, 1, 0, 0, 0, 0, pacific), `tier("standard", p * 1.5 + cr * 0.075 + c * 7.5)`},
	} {
		expr, ok := builtinBillingExprAt(tc.model, tc.at)
		require.True(t, ok, tc.model)
		assert.Equal(t, tc.expect, expr, "%s at %s", tc.model, tc.at)
	}
	_, ok := builtinBillingExprAt("gemini-3.6-flash", time.Now())
	assert.False(t, ok)
}

func TestBuiltinBillingExpressionsCompile(t *testing.T) {
	for model, expr := range builtinBillingExpr {
		_, ok := builtinBillingSchedule[model]
		assert.False(t, ok, "%s is in both built-in tables", model)
		_, err := billingexpr.CompileFromCache(expr)
		assert.NoError(t, err, model)
	}
	for model, entries := range builtinBillingSchedule {
		require.NotEmpty(t, entries, model)
		assert.True(t, entries[0].From.IsZero(), model)
		for index, entry := range entries {
			if index > 0 {
				assert.True(t, entry.From.After(entries[index-1].From), model)
			}
			_, err := billingexpr.CompileFromCache(entry.Expr)
			assert.NoError(t, err, model)
		}
	}
}
