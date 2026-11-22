package service

import (
	"context"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
)

// BatchSettlement is the final charge of a batch compared with what was
// reserved when it was submitted.
type BatchSettlement struct {
	BatchId          string
	UserId           int
	TokenId          int
	ChannelId        int
	Group            string
	ModelName        string
	BillingSource    string
	SubscriptionId   int
	PreConsumed      int
	Actual           int
	PromptTokens     int
	CompletionTokens int
	Content          []*common.Message
	Other            *model.LogOther
}

// ReleaseBatchReservation returns the whole reservation of a batch that never
// reached the upstream. Such a batch was neither logged nor counted in used
// quota, so only the funding source and the token get the quota back. The
// caller guarantees it runs at most once per batch.
func ReleaseBatchReservation(ctx context.Context, s BatchSettlement) error {
	if s.PreConsumed <= 0 {
		return nil
	}
	if err := AdjustAsyncFunding(s.UserId, s.BillingSource, s.SubscriptionId, -s.PreConsumed); err != nil {
		return err
	}
	AdjustAsyncTokenQuota(ctx, s.TokenId, s.BatchId, -s.PreConsumed)
	return nil
}

// SettleBatchCharge charges or refunds the difference between the actual
// and the reserved quota of a batch, on the funding source and token used at
// submission, and records it in the user log. The caller guarantees it runs
// at most once per batch.
func SettleBatchCharge(ctx context.Context, s BatchSettlement) error {
	delta := s.Actual - s.PreConsumed
	if delta != 0 {
		if err := AdjustAsyncFunding(s.UserId, s.BillingSource, s.SubscriptionId, delta); err != nil {
			logger.LogError(ctx, common.LogText("batch %s failed to adjust funding by %d (actual %d, pre-consumed %d): %s",
				s.BatchId, delta, s.Actual, s.PreConsumed, err.Error()))
			return err
		}
		AdjustAsyncTokenQuota(ctx, s.TokenId, s.BatchId, delta)
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
	return nil
}
