package model

import (
	"errors"

	"gorm.io/gorm"
)

// Batch statuses follow the OpenAI Batch object.
const (
	BatchStatusValidating = "validating"
	BatchStatusFailed     = "failed"
	BatchStatusInProgress = "in_progress"
	BatchStatusFinalizing = "finalizing"
	BatchStatusCompleted  = "completed"
	BatchStatusExpired    = "expired"
	BatchStatusCancelling = "cancelling"
	BatchStatusCancelled  = "cancelled"
)

const (
	BatchBillingReserved = "reserved"
	BatchBillingSettled  = "settled"
)

const (
	BatchFilePurposeInput  = "batch"
	BatchFilePurposeOutput = "batch_output"
)

var unfinishedBatchStatuses = []string{
	BatchStatusValidating,
	BatchStatusInProgress,
	BatchStatusFinalizing,
	BatchStatusCancelling,
}

var ErrBatchFileInUse = errors.New("batch file is the input of an unfinished batch")

func IsBatchTerminalStatus(status string) bool {
	switch status {
	case BatchStatusCompleted, BatchStatusFailed, BatchStatusExpired, BatchStatusCancelled:
		return true
	}
	return false
}

// BatchFile stores an uploaded batch input or a generated output/error file.
// Content lives in the database so every node can serve it.
type BatchFile struct {
	Id        int    `json:"-" gorm:"primaryKey"`
	FileId    string `json:"id" gorm:"type:varchar(64);uniqueIndex"`
	UserId    int    `json:"-" gorm:"index"`
	TokenId   int    `json:"-"`
	Purpose   string `json:"purpose" gorm:"type:varchar(32)"`
	Filename  string `json:"filename" gorm:"type:varchar(255)"`
	Bytes     int64  `json:"bytes"`
	LineCount int    `json:"-"`
	BatchId   string `json:"-" gorm:"type:varchar(64);index"`
	Content   []byte `json:"-" gorm:"not null"`
	CreatedAt int64  `json:"created_at" gorm:"bigint"`
	ExpiresAt int64  `json:"expires_at" gorm:"bigint;index"`
}

// Batch is one batch submitted to an upstream native batch API. The upstream
// batch belongs to one channel key, so the channel and a fingerprint of the
// key are kept to address the same upstream account later.
type Batch struct {
	Id               int    `gorm:"primaryKey"`
	BatchId          string `gorm:"type:varchar(64);uniqueIndex"`
	UserId           int    `gorm:"index"`
	TokenId          int
	UsingGroup       string `gorm:"type:varchar(64)"`
	Endpoint         string `gorm:"type:varchar(64)"`
	Model            string `gorm:"type:varchar(255)"`
	UpstreamModel    string `gorm:"type:varchar(255)"`
	InputFileId      string `gorm:"type:varchar(64);index"`
	CompletionWindow string `gorm:"type:varchar(16)"`
	Metadata         string `gorm:"type:text"`

	ChannelId       int `gorm:"index"`
	ChannelType     int
	KeyFingerprint  string `gorm:"type:varchar(64)"`
	UpstreamBatchId string `gorm:"type:varchar(255)"`
	UpstreamState   string `gorm:"type:text"`

	Status           string `gorm:"type:varchar(32);index"`
	RequestTotal     int
	RequestCompleted int
	RequestFailed    int
	Errors           string `gorm:"type:text"`
	OutputFileId     string `gorm:"type:varchar(64)"`
	ErrorFileId      string `gorm:"type:varchar(64)"`

	CreatedAt    int64 `gorm:"bigint"`
	InProgressAt int64 `gorm:"bigint"`
	FinalizingAt int64 `gorm:"bigint"`
	CompletedAt  int64 `gorm:"bigint"`
	FailedAt     int64 `gorm:"bigint"`
	ExpiredAt    int64 `gorm:"bigint"`
	CancellingAt int64 `gorm:"bigint"`
	CancelledAt  int64 `gorm:"bigint"`
	ExpiresAt    int64 `gorm:"bigint"`
	UpdatedAt    int64 `gorm:"bigint"`

	NextPollAt   int64 `gorm:"bigint;index"`
	PollFailures int

	Quota          int
	BillingState   string `gorm:"type:varchar(16);index"`
	BillingSource  string `gorm:"type:varchar(16)"`
	SubscriptionId int
	BillingContext string `gorm:"type:text"`
	UsageSummary   string `gorm:"type:text"`
}

func CreateBatchFile(file *BatchFile) error {
	return DB.Create(file).Error
}

// GetBatchFile returns the file of a user. Content is loaded only on request.
func GetBatchFile(userId int, fileId string, withContent bool) (*BatchFile, error) {
	file := &BatchFile{}
	query := DB.Where("file_id = ? AND user_id = ?", fileId, userId)
	if !withContent {
		query = query.Omit("content")
	}
	if err := query.First(file).Error; err != nil {
		return nil, err
	}
	return file, nil
}

// ListBatchFiles lists the files of a user, newest first. after is the id of
// the last file of the previous page.
func ListBatchFiles(userId int, purpose string, after string, limit int) ([]*BatchFile, bool, error) {
	query := DB.Model(&BatchFile{}).Omit("content").Where("user_id = ?", userId)
	if purpose != "" {
		query = query.Where("purpose = ?", purpose)
	}
	if after != "" {
		cursor := &BatchFile{}
		if err := DB.Select("id").Where("file_id = ? AND user_id = ?", after, userId).First(cursor).Error; err != nil {
			return nil, false, err
		}
		query = query.Where("id < ?", cursor.Id)
	}
	var files []*BatchFile
	if err := query.Order("id desc").Limit(limit + 1).Find(&files).Error; err != nil {
		return nil, false, err
	}
	hasMore := len(files) > limit
	if hasMore {
		files = files[:limit]
	}
	return files, hasMore, nil
}

// DeleteBatchFile deletes a file unless it is the input of an unfinished or
// unsettled batch, whose settlement still reads it.
func DeleteBatchFile(userId int, fileId string) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		file := &BatchFile{}
		if err := lockForUpdate(tx).Select("id").Where("file_id = ? AND user_id = ?", fileId, userId).First(file).Error; err != nil {
			return err
		}
		var inUse int64
		if err := tx.Model(&Batch{}).
			Where("input_file_id = ? AND (status IN ? OR billing_state = ?)", fileId, unfinishedBatchStatuses, BatchBillingReserved).
			Count(&inUse).Error; err != nil {
			return err
		}
		if inUse > 0 {
			return ErrBatchFileInUse
		}
		return tx.Delete(&BatchFile{}, file.Id).Error
	})
}

// HasExpiredBatchFiles reports whether a file's retention has ended.
func HasExpiredBatchFiles(now int64) bool {
	var id int
	err := DB.Model(&BatchFile{}).Where("expires_at > 0 AND expires_at <= ?", now).Limit(1).Pluck("id", &id).Error
	return err == nil && id != 0
}

// DeleteExpiredBatchFiles removes up to limit files whose retention ended,
// except the inputs of batches not yet settled, whose settlement reads them.
func DeleteExpiredBatchFiles(now int64, limit int) (int64, error) {
	var ids []int
	inUse := DB.Model(&Batch{}).Select("input_file_id").
		Where("status IN ? OR billing_state = ?", unfinishedBatchStatuses, BatchBillingReserved)
	if err := DB.Model(&BatchFile{}).Where("expires_at > 0 AND expires_at <= ?", now).
		Where("file_id NOT IN (?)", inUse).Limit(limit).Pluck("id", &ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	result := DB.Where("id IN ?", ids).Delete(&BatchFile{})
	return result.RowsAffected, result.Error
}

func CreateBatch(batch *Batch) error {
	return DB.Create(batch).Error
}

func GetBatch(userId int, batchId string) (*Batch, error) {
	batch := &Batch{}
	if err := DB.Where("batch_id = ? AND user_id = ?", batchId, userId).First(batch).Error; err != nil {
		return nil, err
	}
	return batch, nil
}

func GetBatchByBatchId(batchId string) (*Batch, error) {
	batch := &Batch{}
	if err := DB.Where("batch_id = ?", batchId).First(batch).Error; err != nil {
		return nil, err
	}
	return batch, nil
}

func ListBatches(userId int, after string, limit int) ([]*Batch, bool, error) {
	query := DB.Model(&Batch{}).Where("user_id = ?", userId)
	if after != "" {
		cursor := &Batch{}
		if err := DB.Select("id").Where("batch_id = ? AND user_id = ?", after, userId).First(cursor).Error; err != nil {
			return nil, false, err
		}
		query = query.Where("id < ?", cursor.Id)
	}
	var batches []*Batch
	if err := query.Order("id desc").Limit(limit + 1).Find(&batches).Error; err != nil {
		return nil, false, err
	}
	hasMore := len(batches) > limit
	if hasMore {
		batches = batches[:limit]
	}
	return batches, hasMore, nil
}

// HasUnfinishedBatches reports whether the poll task has work, either an
// unfinished batch or a terminal one whose charge is not settled yet.
func HasUnfinishedBatches() bool {
	var id int
	err := DB.Model(&Batch{}).
		Where("status IN ? OR billing_state = ?", unfinishedBatchStatuses, BatchBillingReserved).
		Limit(1).Pluck("id", &id).Error
	return err == nil && id != 0
}

// FindDueBatches returns batches whose next poll time has come.
func FindDueBatches(now int64, limit int) ([]*Batch, error) {
	var batches []*Batch
	err := DB.Where("(status IN ? OR billing_state = ?) AND next_poll_at <= ?", unfinishedBatchStatuses, BatchBillingReserved, now).
		Order("next_poll_at asc").Limit(limit).Find(&batches).Error
	return batches, err
}

// UpdateBatchIfStatus applies updates only while the batch still has
// fromStatus, so concurrent writers cannot move it backwards.
func UpdateBatchIfStatus(batchId string, fromStatus string, updates map[string]any) (bool, error) {
	updates["updated_at"] = GetDBTimestamp()
	result := DB.Model(&Batch{}).Where("batch_id = ? AND status = ?", batchId, fromStatus).Updates(updates)
	return result.RowsAffected > 0, result.Error
}

// SettleBatch records the terminal state, the output files and the final
// charge in one transaction. It succeeds only for the caller that moves the
// charge from reserved to settled, so funds are adjusted at most once.
func SettleBatch(batchId string, updates map[string]any, files []*BatchFile) (bool, error) {
	won := false
	updates["billing_state"] = BatchBillingSettled
	updates["updated_at"] = GetDBTimestamp()
	err := DB.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&Batch{}).
			Where("batch_id = ? AND billing_state = ?", batchId, BatchBillingReserved).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		for _, file := range files {
			if err := tx.Create(file).Error; err != nil {
				return err
			}
		}
		won = true
		return nil
	})
	return won, err
}

// DeleteUnsubmittedBatch removes the record of a batch that never reached the
// upstream once its reservation is settled.
func DeleteUnsubmittedBatch(batchId string) error {
	return DB.Where("batch_id = ? AND upstream_batch_id = ? AND billing_state = ?", batchId, "", BatchBillingSettled).
		Delete(&Batch{}).Error
}

// DeleteSettledBatch deletes a finished and settled batch of a user.
func DeleteSettledBatch(userId int, batchId string) (bool, error) {
	result := DB.Where("batch_id = ? AND user_id = ? AND status IN ? AND billing_state = ?",
		batchId, userId,
		[]string{BatchStatusCompleted, BatchStatusFailed, BatchStatusExpired, BatchStatusCancelled},
		BatchBillingSettled).Delete(&Batch{})
	return result.RowsAffected > 0, result.Error
}
