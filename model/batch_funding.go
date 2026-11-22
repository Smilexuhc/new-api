package model

import (
	"errors"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

// BatchFunding is the money a batch settlement moves: Delta > 0 charges the
// user beyond the reservation, Delta < 0 refunds part of it.
type BatchFunding struct {
	UserId         int
	TokenId        int
	SubscriptionId int // set when a subscription paid the reservation
	Delta          int
}

// BatchFundingResult is what a settlement moved, so caches follow the
// committed rows.
type BatchFundingResult struct {
	UserId            int
	WalletDelta       int // charged from the wallet; negative is a refund
	SubscriptionDelta int // added to the subscription usage
	TokenId           int
	TokenKey          string
	TokenDelta        int
	// Uncollected is the part of a charge the paying subscription could not
	// cover and does not let the wallet pay.
	Uncollected int
}

// applyBatchFunding moves the settlement delta inside the settlement
// transaction. A charge that the paying subscription cannot cover falls to
// the wallet, which may go negative like a synchronous post-consume charge,
// unless the subscription forbids it; then that part goes uncollected. A
// charge for a subscription that no longer exists falls to the wallet.
func applyBatchFunding(tx *gorm.DB, f *BatchFunding) (*BatchFundingResult, error) {
	result := &BatchFundingResult{UserId: f.UserId, TokenId: f.TokenId}
	if f.Delta == 0 {
		return result, nil
	}
	walletDelta := f.Delta
	if f.SubscriptionId > 0 {
		var sub UserSubscription
		err := lockForUpdate(tx).Where("id = ?", f.SubscriptionId).First(&sub).Error
		switch {
		case err == nil:
			subDelta := f.Delta
			if subDelta > 0 && sub.AmountTotal > 0 {
				subDelta = int(min(int64(subDelta), max(sub.AmountTotal-sub.AmountUsed, 0)))
			}
			if subDelta < 0 {
				subDelta = int(max(int64(subDelta), -sub.AmountUsed))
			}
			if subDelta != 0 {
				if err := tx.Model(&UserSubscription{}).Where("id = ?", sub.Id).
					Update("amount_used", gorm.Expr("amount_used + ?", subDelta)).Error; err != nil {
					return nil, err
				}
			}
			result.SubscriptionDelta = subDelta
			if f.Delta > 0 {
				walletDelta = f.Delta - subDelta
				if !sub.AllowWalletOverflow {
					result.Uncollected = walletDelta
					walletDelta = 0
				}
			} else {
				walletDelta = 0
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			if f.Delta < 0 {
				walletDelta = 0
			}
		default:
			return nil, err
		}
	}
	if walletDelta != 0 {
		if err := tx.Model(&User{}).Where("id = ?", f.UserId).
			Update("quota", gorm.Expr("quota - ?", walletDelta)).Error; err != nil {
			return nil, err
		}
		result.WalletDelta = walletDelta
	}
	if tokenDelta := f.Delta - result.Uncollected; f.TokenId > 0 && tokenDelta != 0 {
		var token Token
		err := tx.Where("id = ?", f.TokenId).First(&token).Error
		switch {
		case err == nil:
			if err := tx.Model(&Token{}).Where("id = ?", f.TokenId).Updates(map[string]any{
				"remain_quota":  gorm.Expr("remain_quota - ?", tokenDelta),
				"used_quota":    gorm.Expr("used_quota + ?", tokenDelta),
				"accessed_time": common.GetTimestamp(),
			}).Error; err != nil {
				return nil, err
			}
			result.TokenKey = token.Key
			result.TokenDelta = tokenDelta
		case errors.Is(err, gorm.ErrRecordNotFound):
		default:
			return nil, err
		}
	}
	return result, nil
}

// syncCaches applies a committed settlement to the quota caches.
func (r *BatchFundingResult) syncCaches() {
	if r == nil || !common.RedisEnabled {
		return
	}
	if r.WalletDelta != 0 {
		if err := cacheDecrUserQuota(r.UserId, int64(r.WalletDelta)); err != nil {
			common.SysLog(common.LogText("batch settlement: failed to sync the user quota cache: %s", err.Error()))
		}
	}
	if r.TokenDelta != 0 && r.TokenKey != "" {
		if _, err := cacheApplyTokenQuotaDelta(r.TokenId, r.TokenKey, int64(-r.TokenDelta)); err != nil {
			common.SysLog(common.LogText("batch settlement: failed to sync the token quota cache: %s", err.Error()))
		}
	}
}
