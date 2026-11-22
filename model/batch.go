package model

import (
	"bytes"
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

// A batch holds its reservation while reserved and moves money exactly once
// when it leaves that state: to settled, or, when its upstream outcome cannot
// be read, to held, where the reservation stays until an administrator
// resolves it.
const (
	BatchBillingReserved = "reserved"
	BatchBillingHeld     = "held"
	BatchBillingSettled  = "settled"
)

const (
	BatchFilePurposeInput  = "batch"
	BatchFilePurposeOutput = "batch_output"
)

// batchFilePartSize bounds the content written in one statement, well below
// the smallest MySQL max_allowed_packet default (4 MiB on 5.7).
const batchFilePartSize = 1 << 20

var unfinishedBatchStatuses = []string{
	BatchStatusValidating,
	BatchStatusInProgress,
	BatchStatusFinalizing,
	BatchStatusCancelling,
}

// unsettledBillingStates are the states whose settlement still reads the
// input file.
var unsettledBillingStates = []string{BatchBillingReserved, BatchBillingHeld}

var ErrBatchFileInUse = errors.New("batch file is the input of an unfinished batch")

func IsBatchTerminalStatus(status string) bool {
	switch status {
	case BatchStatusCompleted, BatchStatusFailed, BatchStatusExpired, BatchStatusCancelled:
		return true
	}
	return false
}

// BatchFile stores an uploaded batch input or a generated output/error file.
// Content lives in the database so every node can serve it; content larger
// than one part is kept in BatchFilePart rows and Content stays empty.
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
	Parts     int    `json:"-"`
	CreatedAt int64  `json:"created_at" gorm:"bigint"`
	ExpiresAt int64  `json:"expires_at" gorm:"bigint;index"`
}

// BatchFilePart is one piece of the content of a large batch file.
type BatchFilePart struct {
	Id      int    `gorm:"primaryKey"`
	FileId  string `gorm:"type:varchar(64);index:idx_batch_file_part,priority:1"`
	Seq     int    `gorm:"index:idx_batch_file_part,priority:2"`
	Content []byte `gorm:"not null"`
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

	NextPollAt    int64 `gorm:"bigint;index"`
	PollFailures  int
	NotFoundPolls int
	// SubmissionUnrecorded is set until the submission is in the usage
	// statistics; a batch that never reached the upstream stays uncounted.
	SubmissionUnrecorded bool

	Quota          int
	BillingState   string `gorm:"type:varchar(16);index"`
	BillingSource  string `gorm:"type:varchar(16)"`
	SubscriptionId int
	BillingContext string `gorm:"type:text"`
	UsageSummary   string `gorm:"type:text"`
}

func CreateBatchFile(file *BatchFile) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		return createBatchFile(tx, file)
	})
}

// createBatchFile writes a file, splitting content larger than one part.
func createBatchFile(tx *gorm.DB, file *BatchFile) error {
	content := file.Content
	if len(content) <= batchFilePartSize {
		file.Parts = 0
		return tx.Create(file).Error
	}
	file.Content = []byte{}
	file.Parts = (len(content) + batchFilePartSize - 1) / batchFilePartSize
	defer func() { file.Content = content }()
	if err := tx.Create(file).Error; err != nil {
		return err
	}
	for seq := 0; seq < file.Parts; seq++ {
		end := min((seq+1)*batchFilePartSize, len(content))
		part := &BatchFilePart{FileId: file.FileId, Seq: seq, Content: content[seq*batchFilePartSize : end]}
		if err := tx.Create(part).Error; err != nil {
			return err
		}
	}
	return nil
}

func loadBatchFileParts(file *BatchFile) error {
	if file.Parts == 0 {
		return nil
	}
	var parts []*BatchFilePart
	if err := DB.Where("file_id = ?", file.FileId).Order("seq asc").Find(&parts).Error; err != nil {
		return err
	}
	if len(parts) != file.Parts {
		return errors.New("batch file content is incomplete")
	}
	var content bytes.Buffer
	content.Grow(int(file.Bytes))
	for _, part := range parts {
		content.Write(part.Content)
	}
	file.Content = content.Bytes()
	return nil
}

func deleteBatchFileParts(tx *gorm.DB, fileIds []string) error {
	if len(fileIds) == 0 {
		return nil
	}
	return tx.Where("file_id IN ?", fileIds).Delete(&BatchFilePart{}).Error
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
	if withContent {
		if err := loadBatchFileParts(file); err != nil {
			return nil, err
		}
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

// CountLiveBatchInputFiles counts the input files of a user that have not
// expired.
func CountLiveBatchInputFiles(userId int, now int64) (int64, error) {
	var count int64
	err := DB.Model(&BatchFile{}).
		Where("user_id = ? AND purpose = ? AND (expires_at = 0 OR expires_at > ?)", userId, BatchFilePurposeInput, now).
		Count(&count).Error
	return count, err
}

// DeleteBatchFile deletes a file unless it is the input of an unfinished or
// unsettled batch, whose settlement still reads it.
func DeleteBatchFile(userId int, fileId string) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		file := &BatchFile{}
		if err := lockForUpdate(tx).Select("id", "file_id").Where("file_id = ? AND user_id = ?", fileId, userId).First(file).Error; err != nil {
			return err
		}
		var inUse int64
		if err := tx.Model(&Batch{}).
			Where("input_file_id = ? AND (status IN ? OR billing_state IN ?)", fileId, unfinishedBatchStatuses, unsettledBillingStates).
			Count(&inUse).Error; err != nil {
			return err
		}
		if inUse > 0 {
			return ErrBatchFileInUse
		}
		if err := deleteBatchFileParts(tx, []string{file.FileId}); err != nil {
			return err
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
	var files []*BatchFile
	inUse := DB.Model(&Batch{}).Select("input_file_id").
		Where("status IN ? OR billing_state IN ?", unfinishedBatchStatuses, unsettledBillingStates)
	if err := DB.Model(&BatchFile{}).Select("id", "file_id").Where("expires_at > 0 AND expires_at <= ?", now).
		Where("file_id NOT IN (?)", inUse).Limit(limit).Find(&files).Error; err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, nil
	}
	ids := make([]int, 0, len(files))
	fileIds := make([]string, 0, len(files))
	for _, file := range files {
		ids = append(ids, file.Id)
		fileIds = append(fileIds, file.FileId)
	}
	var deleted int64
	err := DB.Transaction(func(tx *gorm.DB) error {
		if err := deleteBatchFileParts(tx, fileIds); err != nil {
			return err
		}
		result := tx.Where("id IN ?", ids).Delete(&BatchFile{})
		deleted = result.RowsAffected
		return result.Error
	})
	return deleted, err
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

// ListHeldBatches lists the batches waiting for an administrator, oldest
// first.
func ListHeldBatches(limit int) ([]*Batch, error) {
	var batches []*Batch
	err := DB.Where("billing_state = ?", BatchBillingHeld).Order("id asc").Limit(limit).Find(&batches).Error
	return batches, err
}

// HasUnfinishedBatches reports whether the poll task has work: a batch that
// still holds its reservation and is not waiting for an administrator.
func HasUnfinishedBatches() bool {
	var id int
	err := DB.Model(&Batch{}).Where("billing_state = ?", BatchBillingReserved).Limit(1).Pluck("id", &id).Error
	return err == nil && id != 0
}

// FindDueBatches returns batches whose next poll time has come. A held batch
// waits for an administrator and is not polled.
func FindDueBatches(now int64, limit int) ([]*Batch, error) {
	var batches []*Batch
	err := DB.Where("billing_state = ? AND next_poll_at <= ?", BatchBillingReserved, now).
		Order("next_poll_at asc").Limit(limit).Find(&batches).Error
	return batches, err
}

// UpdateBatchIfStatus applies updates only while the batch still has
// fromStatus and is reserved, so concurrent writers cannot move it backwards
// or touch a batch another node settled or held.
func UpdateBatchIfStatus(batchId string, fromStatus string, updates map[string]any) (bool, error) {
	updates["updated_at"] = GetDBTimestamp()
	result := DB.Model(&Batch{}).Where("batch_id = ? AND status = ? AND billing_state = ?", batchId, fromStatus, BatchBillingReserved).Updates(updates)
	return result.RowsAffected > 0, result.Error
}

// DelayBatchPoll pushes back the next poll of a reserved batch. It writes no
// other column, so it succeeds when a larger update was rejected for its
// content.
func DelayBatchPoll(batchId string, pollFailures int, nextPollAt int64) error {
	return DB.Model(&Batch{}).Where("batch_id = ? AND billing_state = ?", batchId, BatchBillingReserved).
		Updates(map[string]any{"poll_failures": pollFailures, "next_poll_at": nextPollAt, "updated_at": GetDBTimestamp()}).Error
}

// SettleBatch records the terminal state, the output files and the final
// charge, and moves the money, in one transaction. It succeeds only for the
// caller that moves the batch from fromState to settled, so funds move at
// most once, and a failure leaves both the batch and the money unchanged.
func SettleBatch(batchId string, fromState string, updates map[string]any, files []*BatchFile, funding *BatchFunding) (bool, *BatchFundingResult, error) {
	won := false
	var applied *BatchFundingResult
	updates["billing_state"] = BatchBillingSettled
	updates["updated_at"] = GetDBTimestamp()
	err := DB.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&Batch{}).
			Where("batch_id = ? AND billing_state = ?", batchId, fromState).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		for _, file := range files {
			if err := createBatchFile(tx, file); err != nil {
				return err
			}
		}
		if funding != nil {
			var err error
			if applied, err = applyBatchFunding(tx, funding); err != nil {
				return err
			}
		}
		won = true
		return nil
	})
	if err != nil || !won {
		return false, nil, err
	}
	applied.syncCaches()
	return true, applied, nil
}

// HoldBatch moves a reserved batch to held with updates, keeping its
// reservation for an administrator. It reports whether this call did.
func HoldBatch(batchId string, updates map[string]any) (bool, error) {
	updates["billing_state"] = BatchBillingHeld
	updates["updated_at"] = GetDBTimestamp()
	result := DB.Model(&Batch{}).Where("batch_id = ? AND billing_state = ?", batchId, BatchBillingReserved).Updates(updates)
	return result.RowsAffected > 0, result.Error
}

// ResumeHeldBatch returns a held batch to polling with updates.
func ResumeHeldBatch(batchId string, updates map[string]any) (bool, error) {
	updates["billing_state"] = BatchBillingReserved
	updates["updated_at"] = GetDBTimestamp()
	result := DB.Model(&Batch{}).Where("batch_id = ? AND billing_state = ?", batchId, BatchBillingHeld).Updates(updates)
	return result.RowsAffected > 0, result.Error
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
