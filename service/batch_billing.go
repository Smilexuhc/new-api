package service

import (
	"context"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
)

// BatchSettlement is the final charge of a batch compared with what was
// reserved when it was submitted.
type BatchSettlement struct {
	BatchId        string
	UserId         int
	TokenId        int
	ChannelId      int
	Group          string
	ModelName      string
	BillingSource  string
	SubscriptionId int
	PreConsumed    int
	Actual         int
	// Uncounted is a batch whose submission is not in the usage statistics.
	Uncounted        bool
	PromptTokens     int
	CompletionTokens int
	Content          []*common.Message
	Other            *model.LogOther
}

// BatchFunding is the money the settlement moves, on the funding source and
// token used at submission. model.SettleBatch applies it in the transaction
// that settles the batch.
func BatchFunding(s BatchSettlement) *model.BatchFunding {
	funding := &model.BatchFunding{UserId: s.UserId, TokenId: s.TokenId, Delta: s.Actual - s.PreConsumed}
	if s.BillingSource == BillingSourceSubscription {
		funding.SubscriptionId = s.SubscriptionId
	}
	return funding
}

// RecordBatchSettlement updates the usage statistics and writes the user log
// of a settlement whose funds were moved. A counted batch logged and counted
// its reservation at submission, so it records the difference; an uncounted
// one records its final charge, and nothing if it is free.
func RecordBatchSettlement(ctx context.Context, s BatchSettlement, applied *model.BatchFundingResult) {
	delta := s.Actual - s.PreConsumed
	if s.Uncounted {
		if s.Actual <= 0 {
			return
		}
		delta = s.Actual
		model.UpdateUserUsedQuotaAndRequestCount(s.UserId, s.Actual)
		model.UpdateChannelUsedQuota(s.ChannelId, s.Actual)
	} else if delta != 0 {
		// Submission counted the request; settlement only moves the quota.
		model.UpdateUserUsedQuota(s.UserId, delta)
		model.UpdateChannelUsedQuota(s.ChannelId, delta)
	}
	logType := model.LogTypeConsume
	logQuota := delta
	if delta < 0 {
		logType = model.LogTypeRefund
		logQuota = -delta
	}
	other := s.Other
	if other == nil {
		other = model.NewLogOther()
	}
	other.SetPublic("batch_id", s.BatchId)
	other.SetPublic("is_batch", true)
	other.SetPublic("pre_consumed_quota", s.PreConsumed)
	other.SetPublic("actual_quota", s.Actual)
	if applied != nil && s.BillingSource == BillingSourceSubscription && delta > 0 && applied.WalletDelta > 0 {
		other.SetPublic("wallet_overflow_quota", applied.WalletDelta)
	}
	model.RecordTaskBillingLog(model.RecordTaskBillingLogParams{
		UserId:           s.UserId,
		LogType:          logType,
		Content:          s.Content,
		ChannelId:        s.ChannelId,
		ModelName:        s.ModelName,
		Quota:            logQuota,
		TokenId:          s.TokenId,
		Group:            s.Group,
		Other:            other,
		PromptTokens:     s.PromptTokens,
		CompletionTokens: s.CompletionTokens,
	})
}
