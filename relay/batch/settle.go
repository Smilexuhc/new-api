package batch

import (
	"bytes"
	"strconv"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

// usageSummary is stored with a finished batch. Only the token counts and
// model versions are shown to the user; the charge and the upstream cost are
// for reconciliation.
type usageSummary struct {
	Succeeded        int            `json:"succeeded"`
	Failed           int            `json:"failed"`
	NotProcessed     int            `json:"not_processed"`
	EstimatedItems   int            `json:"estimated_items"`
	PromptTokens     int            `json:"prompt_tokens"`
	CompletionTokens int            `json:"completion_tokens"`
	CachedTokens     int            `json:"cached_tokens"`
	AudioTokens      int            `json:"audio_tokens"`
	ReasoningTokens  int            `json:"reasoning_tokens"`
	Quota            int            `json:"quota"`
	PreConsumed      int            `json:"pre_consumed"`
	BatchRatio       float64        `json:"batch_ratio"`
	UpstreamCostUSD  *float64       `json:"upstream_cost_usd,omitempty"`
	ModelVersions    map[string]int `json:"model_versions,omitempty"`
	ResolvedModel    string         `json:"resolved_model,omitempty"`
}

func (s usageSummary) view() *BatchUsage {
	return &BatchUsage{
		InputTokens:         s.PromptTokens,
		OutputTokens:        s.CompletionTokens,
		TotalTokens:         s.PromptTokens + s.CompletionTokens,
		InputTokensDetails:  map[string]int{"cached_tokens": s.CachedTokens, "audio_tokens": s.AudioTokens},
		OutputTokensDetails: map[string]int{"reasoning_tokens": s.ReasoningTokens},
		ModelVersions:       s.ModelVersions,
		ResolvedModel:       s.ResolvedModel,
	}
}

type settlementResult struct {
	Summary    usageSummary
	Output     []byte
	Errors     []byte
	QuotaClamp *common.QuotaClamp
}

func (r *settlementResult) summaryJSON() string {
	data, _ := common.Marshal(r.Summary)
	return string(data)
}

func notProcessedCode(status string) (string, string) {
	switch status {
	case model.BatchStatusExpired:
		return "batch_expired", "This request could not be executed before the completion window expired."
	case model.BatchStatusCancelled:
		return "batch_cancelled", "This request was not executed because the batch was cancelled."
	case model.BatchStatusFailed:
		return "batch_failed", "This request was not executed because the batch failed."
	default:
		return "missing_result", "The upstream returned no result for this request."
	}
}

func outputLineId() string {
	suffix, _ := common.GenerateRandomCharsKey(24)
	return "batch_req_" + suffix
}

func writeLine(buffer *bytes.Buffer, line outputLine) {
	data, err := common.Marshal(line)
	if err != nil {
		return
	}
	buffer.Write(data)
	buffer.WriteByte('\n')
}

func resultLine(customId string, result *UpstreamResult) outputLine {
	line := outputLine{Id: outputLineId(), CustomId: customId}
	if result.StatusCode != 0 || len(result.Body) > 0 {
		line.Response = &outputResponse{StatusCode: result.StatusCode, RequestId: result.RequestId, Body: result.Body}
	}
	if !result.Succeeded() {
		code := result.ErrorCode
		if code == "" {
			code = strconv.Itoa(result.StatusCode)
		}
		line.Error = &outputError{Code: code, Message: result.ErrorMessage}
	}
	return line
}

// buildSettlement prices every result of a finished batch and writes its
// output and error files. lines is the input in order, or nil when the input
// file is gone; then a succeeded result without usage is charged the average
// reservation.
func buildSettlement(b *model.Batch, snapshot *billingSnapshot, lines []*inputLine, upstream *UpstreamBatch) *settlementResult {
	c := &gin.Context{}
	info := snapshot.relayInfo()
	estimates, _, _ := estimateCharges(c, snapshot, lines)
	fallbackEstimate := b.Quota / max(b.RequestTotal, 1)

	results := make(map[string]*UpstreamResult, len(upstream.Results))
	order := make([]string, 0, len(upstream.Results))
	for index := range upstream.Results {
		result := &upstream.Results[index]
		customId := result.CustomId
		if customId == "" && result.Index >= 0 && result.Index < len(lines) {
			customId = lines[result.Index].CustomId
		}
		if customId == "" {
			customId = "result-" + strconv.Itoa(index)
		}
		if _, duplicate := results[customId]; duplicate {
			continue
		}
		results[customId] = result
		order = append(order, customId)
	}

	settlement := &settlementResult{Summary: usageSummary{PreConsumed: b.Quota, BatchRatio: snapshot.BatchRatio}}
	summary := &settlement.Summary
	var output, errorsOut bytes.Buffer
	var itemCost float64
	hasItemCost := false

	account := func(customId string, result *UpstreamResult, estimate int) {
		line := resultLine(customId, result)
		if !result.Succeeded() {
			summary.Failed++
			writeLine(&errorsOut, line)
			return
		}
		summary.Succeeded++
		charge := chargeResult(c, info, result, estimate)
		summary.Quota += charge.Quota
		if charge.Estimated {
			summary.EstimatedItems++
		}
		if usage := result.Usage; usage != nil {
			summary.PromptTokens += usage.PromptTokens
			summary.CompletionTokens += usage.CompletionTokens
			summary.CachedTokens += usage.PromptTokensDetails.CachedTokens
			summary.AudioTokens += usage.PromptTokensDetails.AudioTokens
			summary.ReasoningTokens += usage.CompletionTokenDetails.ReasoningTokens
		}
		if result.Model != "" {
			if summary.ModelVersions == nil {
				summary.ModelVersions = make(map[string]int)
			}
			summary.ModelVersions[result.Model]++
		}
		if result.Cost != nil {
			itemCost += *result.Cost
			hasItemCost = true
		}
		writeLine(&output, line)
	}

	for index, line := range lines {
		result, ok := results[line.CustomId]
		if !ok {
			summary.NotProcessed++
			code, message := notProcessedCode(upstream.Status)
			writeLine(&errorsOut, outputLine{Id: outputLineId(), CustomId: line.CustomId, Error: &outputError{Code: code, Message: message}})
			continue
		}
		delete(results, line.CustomId)
		account(line.CustomId, result, estimates[index])
	}
	// Results the input does not list still cost what the upstream billed.
	for _, customId := range order {
		if result, ok := results[customId]; ok {
			account(customId, result, fallbackEstimate)
		}
	}

	switch {
	case upstream.Cost != nil:
		summary.UpstreamCostUSD = upstream.Cost
	case hasItemCost:
		summary.UpstreamCostUSD = &itemCost
	}
	summary.ResolvedModel = upstream.ResolvedModel
	settlement.Output = output.Bytes()
	settlement.Errors = errorsOut.Bytes()
	settlement.QuotaClamp = info.QuotaClamp
	return settlement
}
