package operation_setting

import (
	"math"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/config"
)

const batchSettingPrefix = "batch_setting."

// MaxBatchPriceRatio bounds the administrator batch price ratio so a typo
// cannot multiply every batch charge by an unbounded factor.
const MaxBatchPriceRatio = 10

// BatchSetting configures the OpenAI-compatible /v1/files and /v1/batches
// endpoints, which forward whole batches to upstream native batch APIs.
type BatchSetting struct {
	Enabled bool `json:"enabled"`
	// PriceRatio multiplies the model price of batch requests. 0 uses the
	// discount of the upstream batch API (0.5 for OpenRouter and Gemini), so
	// the gateway keeps the same margin it has on synchronous requests.
	PriceRatio             float64 `json:"price_ratio"`
	MaxRequestsPerBatch    int     `json:"max_requests_per_batch"`
	MaxFileSizeMB          int     `json:"max_file_size_mb"`
	DefaultMaxOutputTokens int     `json:"default_max_output_tokens"`
	PollIntervalSeconds    int     `json:"poll_interval_seconds"`
	FileRetentionDays      int     `json:"file_retention_days"`
}

var batchSetting = BatchSetting{
	MaxRequestsPerBatch:    10000,
	MaxFileSizeMB:          10,
	DefaultMaxOutputTokens: 4096,
	PollIntervalSeconds:    60,
	FileRetentionDays:      30,
}

func init() {
	config.GlobalConfig.Register("batch_setting", &batchSetting)
}

func GetBatchSetting() *BatchSetting {
	return &batchSetting
}

func IsBatchOption(key string) bool {
	return strings.HasPrefix(key, batchSettingPrefix)
}

func ValidateBatchOption(key string, value string) error {
	invalid := common.NewMessage("Invalid batch setting value")
	intInRange := func(minimum, maximum int) error {
		number, err := strconv.Atoi(value)
		if err != nil || number < minimum || number > maximum {
			return invalid
		}
		return nil
	}
	switch strings.TrimPrefix(key, batchSettingPrefix) {
	case "enabled":
		if _, err := strconv.ParseBool(value); err != nil {
			return invalid
		}
	case "price_ratio":
		ratio, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(ratio) || ratio < 0 || ratio > MaxBatchPriceRatio {
			return invalid
		}
	case "max_requests_per_batch":
		return intInRange(1, 50000)
	case "max_file_size_mb":
		return intInRange(1, 200)
	case "default_max_output_tokens":
		return intInRange(1, 1000000)
	case "poll_interval_seconds":
		return intInRange(15, 3600)
	case "file_retention_days":
		return intInRange(1, 3650)
	default:
		return invalid
	}
	return nil
}
