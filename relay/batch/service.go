package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	taskdto "github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	CompletionWindow = "24h"

	maxMetadataPairs       = 16
	maxMetadataKeyLength   = 64
	maxMetadataValueLength = 512

	// submitGracePeriod is how long a batch may stay without an upstream id
	// before the poller treats its submission as lost.
	submitGracePeriod = 10 * time.Minute
	// submitTimeout bounds the upload of a batch to the upstream.
	submitTimeout        = 5 * time.Minute
	submitSaveAttempts   = 3
	submitSaveRetryDelay = 200 * time.Millisecond
	// completionWindowDuration stands in for the expiry of an upstream batch
	// that reports none.
	completionWindowDuration = 24 * time.Hour
	// submitSaveRetryInterval spaces the background saves of a submission
	// whose upstream id could not be recorded.
	submitSaveRetryInterval = 15 * time.Second
	// upstreamLostAfter is how long past its expiry an unfinished batch keeps
	// being polled before it is held.
	upstreamLostAfter = 7 * 24 * time.Hour
	// notFoundPollsBeforeHold is how many polls in a row the upstream must
	// answer not found before a batch is held; a single not found can come
	// from a proxy or a changed base URL.
	notFoundPollsBeforeHold = 4
	// unavailablePollsBeforeHold is how many failed polls a batch whose
	// channel key is gone or rejected gets before it is held.
	unavailablePollsBeforeHold = 4
	// settleFailuresBeforeHold is how many failed polls a batch whose final
	// state cannot be saved gets before it is held.
	settleFailuresBeforeHold = 6
	// retrieveTimeout bounds one read of an upstream batch, results included.
	retrieveTimeout = 5 * time.Minute
	// cancelCheckTimeout bounds the upstream read before a cancellation.
	cancelCheckTimeout = 30 * time.Second
	pollWorkers        = 8
	maxPollBackoff     = 30 * time.Minute
	pollBatchSize      = 100
	// maxRemoteMediaPerBatch bounds the media a batch makes the gateway
	// download.
	maxRemoteMediaPerBatch = 1000
)

func keyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16])
}

func newObjectId(prefix string) (string, error) {
	suffix, err := common.GenerateRandomCharsKey(32)
	if err != nil {
		return "", err
	}
	return prefix + suffix, nil
}

func internalError(err error) *Error {
	common.SysError(common.LogText("batch internal error: %s", err.Error()))
	return newError(http.StatusInternalServerError, "server_error", i18n.MsgBatchInternalError, nil)
}

func notFoundOr(err error, msgID string) *Error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return newError(http.StatusNotFound, "not_found", msgID, nil)
	}
	return internalError(err)
}

func isTerminalStatus(status string) bool {
	return model.IsBatchTerminalStatus(status)
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

func UploadFile(userId int, tokenId int, purpose string, filename string, content []byte) (*model.BatchFile, *Error) {
	if purpose != model.BatchFilePurposeInput {
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidPurpose, nil)
	}
	setting := operation_setting.GetBatchSetting()
	if int64(len(content)) > int64(setting.MaxFileSizeMB)<<20 {
		return nil, newError(http.StatusRequestEntityTooLarge, "invalid_request_error", i18n.MsgBatchFileTooLarge, map[string]any{"MaxMB": setting.MaxFileSizeMB})
	}
	lines, apiErr := parseInput(content, "", setting.MaxRequestsPerBatch)
	if apiErr != nil {
		return nil, apiErr
	}
	now := common.GetTimestamp()
	stored, err := model.CountLiveBatchInputFiles(userId, now)
	if err != nil {
		return nil, internalError(err)
	}
	if stored >= int64(setting.MaxInputFilesPerUser) {
		return nil, newError(http.StatusTooManyRequests, "invalid_request_error", i18n.MsgBatchTooManyFiles, map[string]any{"Max": setting.MaxInputFilesPerUser})
	}
	fileId, err := newObjectId("file-")
	if err != nil {
		return nil, internalError(err)
	}
	file := &model.BatchFile{
		FileId:    fileId,
		UserId:    userId,
		TokenId:   tokenId,
		Purpose:   purpose,
		Filename:  trimFilename(filename),
		Bytes:     int64(len(content)),
		LineCount: len(lines),
		Content:   content,
		CreatedAt: now,
		ExpiresAt: now + int64(setting.FileRetentionDays)*86400,
	}
	if err := model.CreateBatchFile(file); err != nil {
		return nil, internalError(err)
	}
	return file, nil
}

func GetFileContent(userId int, fileId string) (*model.BatchFile, *Error) {
	file, err := model.GetBatchFile(userId, fileId, true)
	if err != nil {
		return nil, notFoundOr(err, i18n.MsgBatchFileNotFound)
	}
	if file.Purpose != model.BatchFilePurposeOutput {
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchFileDownloadForbidden, map[string]any{"Purpose": file.Purpose})
	}
	return file, nil
}

func DeleteFile(userId int, fileId string) *Error {
	err := model.DeleteBatchFile(userId, fileId)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, model.ErrBatchFileInUse):
		return newError(http.StatusConflict, "invalid_request_error", i18n.MsgBatchFileInUse, nil)
	default:
		return notFoundOr(err, i18n.MsgBatchFileNotFound)
	}
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

type CreateRequest struct {
	InputFileId      string            `json:"input_file_id"`
	Endpoint         string            `json:"endpoint"`
	CompletionWindow string            `json:"completion_window"`
	Metadata         map[string]string `json:"metadata"`
}

func validateCreateRequest(req *CreateRequest) *Error {
	if req.InputFileId == "" {
		return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchFileNotFound, nil)
	}
	if req.Endpoint != EndpointChatCompletions {
		return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchUnsupportedEndpoint, map[string]any{"Endpoint": req.Endpoint})
	}
	if req.CompletionWindow != CompletionWindow {
		return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidCompletionWindow, nil)
	}
	if len(req.Metadata) > maxMetadataPairs {
		return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidMetadata, nil)
	}
	for key, value := range req.Metadata {
		if len(key) > maxMetadataKeyLength || len(value) > maxMetadataValueLength {
			return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidMetadata, nil)
		}
	}
	return nil
}

func tokenAllowsModel(c *gin.Context, modelName string) bool {
	if !common.GetContextKeyBool(c, constant.ContextKeyTokenModelLimitEnabled) {
		return true
	}
	value, ok := common.GetContextKey(c, constant.ContextKeyTokenModelLimit)
	if !ok {
		return false
	}
	limit, _ := value.(map[string]bool)
	return middleware.TokenModelLimitAllows(limit, modelName)
}

// selectBatchChannel picks a channel of the user group that serves the model
// and whose type has a native batch adaptor, and loads its context (key,
// base URL, model mapping) like the relay distributor does.
func selectBatchChannel(c *gin.Context, modelName string) (*model.Channel, Adaptor, *Error) {
	group := common.GetContextKeyString(c, constant.ContextKeyUsingGroup)
	noChannel := newError(http.StatusServiceUnavailable, "no_batch_channel", i18n.MsgBatchNoChannel, map[string]any{"Group": group, "Model": modelName})
	service.GetChannelConstraints(c).AddFilter(taskdto.ChannelFilter{
		Kind:         taskdto.FilterChannelTypes,
		ChannelTypes: SupportedChannelTypes(),
	})
	channel, _, selectErr := service.SelectChannelForRequest(c, modelName, &service.RetryParam{
		Ctx:        c,
		ModelName:  modelName,
		TokenGroup: group,
		Retry:      common.GetPointer(0),
	})
	if selectErr != nil || channel == nil {
		return nil, nil, noChannel
	}
	adaptor := GetAdaptor(channel.Type)
	if adaptor == nil {
		return nil, nil, noChannel
	}
	if apiErr := middleware.SetupContextForSelectedChannel(c, channel, modelName); apiErr != nil {
		return nil, nil, &Error{Status: http.StatusServiceUnavailable, Code: "no_batch_channel", Message: apiErr.Error()}
	}
	return channel, adaptor, nil
}

func batchRatioFor(adaptor Adaptor) (float64, string) {
	if ratio := operation_setting.GetBatchSetting().PriceRatio; ratio > 0 {
		return ratio, BatchRatioSourceSetting
	}
	return adaptor.DefaultPriceRatio(), BatchRatioSourceUpstream
}

func billingErrorFrom(apiErr *types.NewAPIError) *Error {
	status := apiErr.StatusCode
	if status == 0 {
		status = http.StatusForbidden
	}
	return &Error{Status: status, Code: string(apiErr.GetErrorCode()), Message: apiErr.Error()}
}

func CreateBatch(c *gin.Context, req *CreateRequest) (*model.Batch, *Error) {
	if apiErr := validateCreateRequest(req); apiErr != nil {
		return nil, apiErr
	}
	userId := c.GetInt("id")
	file, err := model.GetBatchFile(userId, req.InputFileId, true)
	if err != nil {
		return nil, notFoundOr(err, i18n.MsgBatchFileNotFound)
	}
	if file.Purpose != model.BatchFilePurposeInput {
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidPurpose, nil)
	}
	setting := operation_setting.GetBatchSetting()
	lines, apiErr := parseInput(file.Content, req.Endpoint, setting.MaxRequestsPerBatch)
	if apiErr != nil {
		return nil, apiErr
	}
	modelName := lines[0].Model
	if !tokenAllowsModel(c, modelName) {
		return nil, &Error{Status: http.StatusForbidden, Code: "model_not_allowed", Message: i18n.T(c, i18n.MsgDistributorTokenModelForbidden, map[string]any{"Model": modelName})}
	}

	channel, adaptor, apiErr := selectBatchChannel(c, modelName)
	if apiErr != nil {
		return nil, apiErr
	}
	if !adaptor.SupportsEndpoint(req.Endpoint) {
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchUnsupportedEndpoint, map[string]any{"Endpoint": req.Endpoint})
	}

	info := relaycommon.GenRelayInfoOpenAI(c, nil)
	info.OriginModelName = modelName
	info.InitChannelMeta(c)
	if err := helper.ModelMappedHelper(c, info, nil); err != nil {
		return nil, internalError(err)
	}
	// An expression reads request parameters; the create call carries none,
	// so the price check runs on the first request instead.
	info.BillingRequestInput = &billingexpr.RequestInput{Body: lines[0].RawBody}
	priceData, err := helper.ModelPriceHelper(c, info, 0, &types.TokenCountMeta{})
	if err != nil {
		return nil, &Error{Status: http.StatusBadRequest, Code: "model_price_error", Message: err.Error()}
	}
	info.PriceData = priceData
	ratio, ratioSource := batchRatioFor(adaptor)
	snapshot := &billingSnapshot{
		PriceData:              priceData,
		Tiered:                 info.TieredBillingSnapshot,
		BatchRatio:             ratio,
		BatchRatioSource:       ratioSource,
		OriginModel:            modelName,
		BillingModel:           info.GetBillingModelName(),
		UpstreamModel:          info.UpstreamModelName,
		ChannelType:            channel.Type,
		DefaultMaxOutputTokens: setting.DefaultMaxOutputTokens,
	}

	converter, converts := adaptor.(RequestConverter)
	bodies := make([][]byte, 0, len(lines))
	remoteMedia := 0
	for _, line := range lines {
		if err := line.checkUpstreamFields(channel.Type); err != nil {
			return nil, lineError(line.Line, err.Error())
		}
		body, err := line.upstreamBody(info.UpstreamModelName, setting.DefaultMaxOutputTokens)
		if err != nil {
			return nil, lineError(line.Line, err.Error())
		}
		if converts {
			remoteMedia += remoteMediaCount(body)
		}
		bodies = append(bodies, body)
	}
	if remoteMedia > maxRemoteMediaPerBatch {
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchTooManyRemoteMedia, map[string]any{"Max": maxRemoteMediaPerBatch})
	}

	// The reservation is taken before any linked media is downloaded, so a
	// user who cannot pay downloads nothing. Media the adaptor downloads is
	// reserved once its size is known.
	_, preConsume, clamp, err := estimateChargesOf(c, snapshot, lines, bodies, converts)
	if err != nil {
		return nil, &Error{Status: http.StatusBadRequest, Code: "model_price_error", Message: err.Error()}
	}
	info.PriceData.AddOtherRatio(batchRatioKey, ratio)
	info.ForcePreConsume = true
	if apiErr := service.PreConsumeBilling(c, preConsume, info); apiErr != nil {
		return nil, billingErrorFrom(apiErr)
	}
	refund := func() {
		if info.Billing != nil {
			info.Billing.Refund(c)
		}
	}

	if converts {
		// Converted requests inline the media they link to; the batch must
		// fit the inline limit, so downloads stop once they would exceed it.
		service.SetDownloadBudget(c, GeminiInlineRequestLimit)
		inlineBytes := 0
		for index, line := range lines {
			var request dto.GeneralOpenAIRequest
			if err := common.Unmarshal(bodies[index], &request); err != nil {
				refund()
				return nil, lineError(line.Line, err.Error())
			}
			body, err := converter.ConvertRequest(c, info, &request)
			if err != nil {
				refund()
				if isDownloadBudgetError(err) {
					return nil, submitError(ErrGeminiInlineTooLarge)
				}
				return nil, lineError(line.Line, err.Error())
			}
			if inlineBytes += len(body); inlineBytes > GeminiInlineRequestLimit {
				refund()
				return nil, submitError(ErrGeminiInlineTooLarge)
			}
			bodies[index] = body
		}
		// The converted requests carry the media and are priced from it.
		_, converted, convertedClamp, err := estimateCharges(c, snapshot, lines, bodies)
		if err != nil {
			refund()
			return nil, &Error{Status: http.StatusBadRequest, Code: "model_price_error", Message: err.Error()}
		}
		if converted > preConsume {
			if apiErr := service.PreConsumeBilling(c, converted, info); apiErr != nil {
				refund()
				return nil, billingErrorFrom(apiErr)
			}
			preConsume, clamp = converted, convertedClamp
		}
	}
	items := make([]SubmitItem, 0, len(lines))
	for index, line := range lines {
		items = append(items, SubmitItem{CustomId: line.CustomId, Body: bodies[index]})
	}

	batchId, err := newObjectId("batch_")
	if err != nil {
		refund()
		return nil, internalError(err)
	}
	snapshotJSON, err := common.Marshal(snapshot)
	if err != nil {
		refund()
		return nil, internalError(err)
	}
	metadataJSON := ""
	if len(req.Metadata) > 0 {
		data, _ := common.Marshal(req.Metadata)
		metadataJSON = string(data)
	}
	now := common.GetTimestamp()
	key := common.GetContextKeyString(c, constant.ContextKeyChannelKey)
	b := &model.Batch{
		BatchId:          batchId,
		UserId:           userId,
		TokenId:          info.TokenId,
		UsingGroup:       info.UsingGroup,
		Endpoint:         req.Endpoint,
		Model:            modelName,
		UpstreamModel:    info.UpstreamModelName,
		InputFileId:      file.FileId,
		CompletionWindow: req.CompletionWindow,
		Metadata:         metadataJSON,
		ChannelId:        channel.Id,
		ChannelType:      channel.Type,
		KeyFingerprint:   keyFingerprint(key),
		Status:           model.BatchStatusValidating,
		RequestTotal:     len(lines),
		CreatedAt:        now,
		UpdatedAt:        now,
		NextPollAt:       now + int64(submitGracePeriod.Seconds()),
		Quota:            preConsume,
		BillingState:     model.BatchBillingReserved,
		BillingSource:    info.BillingSource,
		SubscriptionId:   info.SubscriptionId,
		BillingContext:   string(snapshotJSON),

		SubmissionUnrecorded: true,
	}
	// The row exists before the upstream call so a crash after submission
	// leaves a record that the poller settles instead of a lost reservation.
	if err := model.CreateBatch(b); err != nil {
		refund()
		return nil, internalError(err)
	}
	// From here the record owns the reservation: every refund goes through
	// its switch out of reserved, so a refund cannot happen twice.
	if err := service.SettleBilling(c, info, preConsume); err != nil {
		logger.LogError(c, common.LogText("batch %s: failed to confirm the reservation: %s", batchId, err.Error()))
	}

	conn := &Connection{
		ChannelType: channel.Type,
		BaseURL:     channelBaseURL(channel),
		Key:         key,
		Proxy:       channel.GetSetting().Proxy,
	}
	// A client that goes away must not abort a submission the upstream may
	// already have accepted.
	submitCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), submitTimeout)
	defer cancel()
	upstream, err := adaptor.Submit(submitCtx, conn, &SubmitRequest{
		BatchId:          batchId,
		Endpoint:         req.Endpoint,
		UpstreamModel:    info.UpstreamModelName,
		CompletionWindow: req.CompletionWindow,
		Items:            items,
	})
	if errors.Is(err, ErrNoUpstreamId) {
		// The upstream may run and bill the batch, which cannot be tracked;
		// the reservation waits for an administrator.
		if holdWith(c, b, "submission_unconfirmed", "the upstream accepted the batch without returning its id",
			map[string]any{"submission_unrecorded": false}) {
			recordSubmission(c, info, b, ratio, ratioSource, clamp)
		}
		return nil, submitError(err)
	}
	if err != nil {
		// The client gets an error and no batch id, so the record goes too.
		if releaseUnsubmitted(c, b, "submission_failed", err.Error()) {
			if deleteErr := model.DeleteUnsubmittedBatch(batchId); deleteErr != nil {
				logger.LogWarn(c, common.LogText("batch %s: failed to delete the record of a rejected submission: %s", batchId, deleteErr.Error()))
			}
		}
		return nil, submitError(err)
	}

	updates := map[string]any{
		"upstream_batch_id": upstream.Id,
		"status":            nonTerminalStatus(upstream.Status, model.BatchStatusValidating),
		"expires_at":        upstream.ExpiresAt,
		"next_poll_at":      now + int64(setting.PollIntervalSeconds),
	}
	if updates["status"] == model.BatchStatusInProgress {
		updates["in_progress_at"] = now
	}
	// The submission is counted with the save that marks it counted; a batch
	// saved later by the retry is counted when it settles.
	recorded := maps.Clone(updates)
	recorded["submission_unrecorded"] = false
	if err := saveSubmission(batchId, recorded); err != nil {
		// The batch runs upstream and is billed once its id is saved. It is
		// cancelled where the upstream can, and the save keeps being retried;
		// a batch still without its id after the grace period is held.
		logger.LogError(c, common.LogText("batch %s: submitted as %s but the record update failed, cancelling it upstream and retrying the save: %s", batchId, upstream.Id, err.Error()))
		if _, cancelErr := adaptor.Cancel(submitCtx, conn, upstream.Id); cancelErr != nil {
			logger.LogError(c, common.LogText("batch %s: failed to cancel upstream batch %s: %s", batchId, upstream.Id, cancelErr.Error()))
		}
		retrySubmissionSave(batchId, upstream.Id, b.CreatedAt, updates)
		return nil, internalError(err)
	}
	recordSubmission(c, info, b, ratio, ratioSource, clamp)

	fresh, err := model.GetBatch(userId, batchId)
	if err != nil {
		return b, nil
	}
	return fresh, nil
}

func isDownloadBudgetError(err error) bool {
	return errors.Is(err, service.ErrDownloadBudgetExceeded) || strings.Contains(err.Error(), service.ErrDownloadBudgetExceeded.Error())
}

// recordSubmission logs the reservation of a batch the upstream accepted and
// counts it in the usage statistics.
func recordSubmission(c *gin.Context, info *relaycommon.RelayInfo, b *model.Batch, ratio float64, ratioSource string, clamp *common.QuotaClamp) {
	recordSubmitLog(c, info, b, ratio, ratioSource, clamp)
	model.UpdateUserUsedQuotaAndRequestCount(b.UserId, b.Quota)
	model.UpdateChannelUsedQuota(b.ChannelId, b.Quota)
}

// saveSubmission records the upstream id of a submitted batch, retrying
// briefly since the batch already runs upstream.
func saveSubmission(batchId string, updates map[string]any) error {
	var err error
	for attempt := 0; attempt < submitSaveAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * submitSaveRetryDelay)
		}
		var saved bool
		if saved, err = model.UpdateBatchIfStatus(batchId, model.BatchStatusValidating, updates); err == nil {
			if saved {
				return nil
			}
			return errors.New("the batch is no longer waiting for its submission")
		}
	}
	return err
}

// retrySubmissionSave keeps saving the upstream id in the background until
// shortly before the grace period ends.
func retrySubmissionSave(batchId string, upstreamId string, createdAt int64, updates map[string]any) {
	deadline := time.Unix(createdAt, 0).Add(submitGracePeriod - submitSaveRetryInterval)
	gopool.Go(func() {
		for time.Now().Before(deadline) {
			time.Sleep(submitSaveRetryInterval)
			saved, err := model.UpdateBatchIfStatus(batchId, model.BatchStatusValidating, maps.Clone(updates))
			if err == nil {
				if saved {
					common.SysLog(common.LogText("batch %s: saved upstream batch %s after retrying", batchId, upstreamId))
				}
				return
			}
		}
		common.SysError(common.LogText("batch %s: could not save upstream batch %s; it will be held for review", batchId, upstreamId))
	})
}

// releaseUnsubmitted marks a batch that never reached the upstream failed and
// refunds its reservation in the same transaction, reporting whether this
// call did. When the record cannot be switched the batch stays reserved and
// the poller holds it after the grace period.
func releaseUnsubmitted(ctx context.Context, b *model.Batch, code string, reason string) bool {
	settlement := batchSettlement(b, 0)
	won, _, err := model.SettleBatch(b.BatchId, model.BatchBillingReserved, map[string]any{
		"status":    model.BatchStatusFailed,
		"failed_at": common.GetTimestamp(),
		"errors":    batchErrorsJSON(code, reason),
		"quota":     0,
	}, nil, service.BatchFunding(settlement))
	if err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to release the reservation of an unsubmitted batch: %s", b.BatchId, err.Error()))
		return false
	}
	return won
}

// nonTerminalStatus keeps a just-submitted batch out of terminal states; a
// batch the upstream finished immediately is finalized by the poller.
func nonTerminalStatus(status string, fallback string) string {
	switch status {
	case model.BatchStatusValidating, model.BatchStatusInProgress, model.BatchStatusFinalizing:
		return status
	}
	return fallback
}

func submitError(err error) *Error {
	if errors.Is(err, ErrGeminiInlineTooLarge) {
		return newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInlineTooLarge, map[string]any{"MaxMB": GeminiInlineRequestLimit >> 20})
	}
	var upstreamErr *UpstreamError
	if errors.As(err, &upstreamErr) {
		status := http.StatusBadGateway
		if upstreamErr.StatusCode >= 400 && upstreamErr.StatusCode < 500 {
			status = upstreamErr.StatusCode
		}
		return newError(status, "upstream_error", i18n.MsgBatchUpstreamError, map[string]any{"Message": upstreamErr.Message})
	}
	if errors.Is(err, ErrNoUpstreamId) {
		return newError(http.StatusBadGateway, "upstream_error", i18n.MsgBatchUpstreamError, map[string]any{"Message": err.Error()})
	}
	common.SysError(common.LogText("batch submission failed: %s", err.Error()))
	return newError(http.StatusBadGateway, "upstream_error", i18n.MsgBatchUpstreamError, map[string]any{"Message": "the upstream could not be reached"})
}

func channelBaseURL(channel *model.Channel) string {
	if base := channel.GetBaseURL(); base != "" {
		return base
	}
	return constant.GetChannelBaseURL(channel.Type)
}

func recordSubmitLog(c *gin.Context, info *relaycommon.RelayInfo, b *model.Batch, ratio float64, ratioSource string, clamp *common.QuotaClamp) {
	other := model.NewLogOther()
	other.SetPublic("is_batch", true)
	other.SetPublic("batch_id", b.BatchId)
	other.SetPublic("request_path", c.Request.URL.Path)
	other.SetPublic("request_count", b.RequestTotal)
	if info.TieredBillingSnapshot != nil {
		noteTieredExpr(info.TieredBillingSnapshot, other)
	} else {
		other.SetPublic("model_price", info.PriceData.ModelPrice)
		if info.PriceData.ModelRatio > 0 {
			other.SetPublic("model_ratio", info.PriceData.ModelRatio)
		}
		other.SetPublic("completion_ratio", info.PriceData.CompletionRatio)
	}
	other.SetPublic("group_ratio", info.PriceData.GroupRatioInfo.GroupRatio)
	other.SetPublic(batchRatioKey, ratio)
	other.SetAdmin("batch_ratio_source", ratioSource)
	if info.IsModelMapped {
		other.SetPublic("is_model_mapped", true)
		other.SetPublic("upstream_model_name", info.UpstreamModelName)
	}
	noteQuotaSaturation(c, b, clamp, other)
	model.RecordConsumeLog(c, b.UserId, model.RecordConsumeLogParams{
		ChannelId: b.ChannelId,
		ModelName: b.Model,
		TokenName: c.GetString("token_name"),
		Quota:     b.Quota,
		Content: []*common.Message{common.NewMessage("Batch {{batch}} submitted with {{count}} requests; the estimated maximum is reserved and settled from upstream usage when the batch ends",
			map[string]any{"batch": b.BatchId, "count": b.RequestTotal})},
		TokenId: b.TokenId,
		Group:   b.UsingGroup,
		Other:   other,
	})
}

// ---------------------------------------------------------------------------
// Poll and finalize
// ---------------------------------------------------------------------------

var errConnectionGone = errors.New("the channel key that submitted the batch is gone")

func resolveConnection(b *model.Batch) (*Connection, Adaptor, error) {
	adaptor := GetAdaptor(b.ChannelType)
	if adaptor == nil {
		return nil, nil, errors.New("no batch adaptor for the channel type")
	}
	channel, err := model.GetChannelById(b.ChannelId, true)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, errConnectionGone
	}
	if err != nil {
		return nil, nil, err
	}
	for _, key := range channel.GetKeys() {
		if keyFingerprint(key) == b.KeyFingerprint {
			return &Connection{
				ChannelType: channel.Type,
				BaseURL:     channelBaseURL(channel),
				Key:         key,
				Proxy:       channel.GetSetting().Proxy,
			}, adaptor, nil
		}
	}
	return nil, nil, errConnectionGone
}

// PollOnce refreshes the batches whose poll time has come, a few at a time so
// one slow upstream does not hold up the others. It returns the number of
// batches it looked at.
func PollOnce(ctx context.Context) int {
	now := common.GetTimestamp()
	batches, err := model.FindDueBatches(now, pollBatchSize)
	if err != nil {
		logger.LogError(ctx, common.LogText("batch poll: failed to load due batches: %s", err.Error()))
		return 0
	}
	workers := make(chan struct{}, pollWorkers)
	var wg sync.WaitGroup
	for _, b := range batches {
		wg.Add(1)
		workers <- struct{}{}
		go func(b *model.Batch) {
			defer wg.Done()
			defer func() { <-workers }()
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.LogError(ctx, common.LogText("batch %s: poll panicked: %v", b.BatchId, recovered))
				}
			}()
			refreshBatch(ctx, b, now)
		}(b)
	}
	wg.Wait()
	if deleted, err := model.DeleteExpiredBatchFiles(now, pollBatchSize); err != nil {
		logger.LogWarn(ctx, common.LogText("batch poll: failed to delete expired files: %s", err.Error()))
	} else if deleted > 0 {
		logger.LogInfo(ctx, common.LogText("batch poll: deleted %d expired files", deleted))
	}
	return len(batches)
}

func pollInterval() int64 {
	return int64(operation_setting.GetBatchSetting().PollIntervalSeconds)
}

func backoffDelay(failures int) int64 {
	delay := pollInterval() << min(failures, 10)
	return min(delay, int64(maxPollBackoff.Seconds()))
}

// backoff pushes back the next poll of a batch whose poll failed.
// notFoundPolls is the number of polls in a row the upstream answered not
// found.
func backoff(ctx context.Context, b *model.Batch, now int64, notFoundPolls int, reason string) {
	failures := b.PollFailures + 1
	delay := backoffDelay(failures)
	logger.LogWarn(ctx, common.LogText("batch %s poll failed (%d in a row), next try in %ds: %s", b.BatchId, failures, delay, reason))
	if _, err := model.UpdateBatchIfStatus(b.BatchId, b.Status, map[string]any{
		"poll_failures":   failures,
		"not_found_polls": notFoundPolls,
		"next_poll_at":    now + delay,
	}); err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to record the poll failure: %s", b.BatchId, err.Error()))
		delayPoll(ctx, b, now)
	}
}

// delayPoll pushes back the next poll after a write of the batch failed,
// writing only the poll columns.
func delayPoll(ctx context.Context, b *model.Batch, now int64) {
	failures := b.PollFailures + 1
	if err := model.DelayBatchPoll(b.BatchId, failures, now+backoffDelay(failures)); err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to delay its next poll: %s", b.BatchId, err.Error()))
	}
}

// upstreamDeadline is when the upstream expires the batch, or the end of its
// completion window when the upstream reports no expiry.
func upstreamDeadline(b *model.Batch) int64 {
	if b.ExpiresAt > 0 {
		return b.ExpiresAt
	}
	return b.CreatedAt + int64(completionWindowDuration.Seconds())
}

func isUpstreamAuthError(err error) bool {
	var upstreamErr *UpstreamError
	return errors.As(err, &upstreamErr) && (upstreamErr.StatusCode == http.StatusUnauthorized || upstreamErr.StatusCode == http.StatusForbidden)
}

func refreshBatch(ctx context.Context, b *model.Batch, now int64) {
	if b.UpstreamBatchId == "" {
		if now-b.CreatedAt >= int64(submitGracePeriod.Seconds()) {
			hold(ctx, b, "submission_unconfirmed", "the submission was not confirmed and may have reached the upstream")
		}
		return
	}
	lost := now > upstreamDeadline(b)+int64(upstreamLostAfter.Seconds())
	conn, adaptor, err := resolveConnection(b)
	if err != nil {
		if lost || (errors.Is(err, errConnectionGone) && b.PollFailures+1 >= unavailablePollsBeforeHold) {
			holdUnreadable(ctx, b, "channel_unavailable", err)
			return
		}
		backoff(ctx, b, now, 0, err.Error())
		return
	}
	retrieveCtx, cancel := context.WithTimeout(ctx, retrieveTimeout)
	defer cancel()
	upstream, err := adaptor.Retrieve(retrieveCtx, conn, b.UpstreamBatchId)
	if err != nil {
		notFoundPolls := 0
		if IsUpstreamNotFound(err) {
			notFoundPolls = b.NotFoundPolls + 1
		}
		switch {
		case lost, notFoundPolls >= notFoundPollsBeforeHold:
			holdUnreadable(ctx, b, "upstream_unavailable", err)
		case isUpstreamAuthError(err) && b.PollFailures+1 >= unavailablePollsBeforeHold:
			holdUnreadable(ctx, b, "channel_unauthorized", err)
		default:
			backoff(ctx, b, now, notFoundPolls, err.Error())
		}
		return
	}
	if upstream.ResultsLost {
		hold(ctx, b, "upstream_results_lost", "the upstream finished requests of the batch but returned no results")
		return
	}
	if isTerminalStatus(upstream.Status) && upstream.ResultsReady {
		finalize(ctx, b, upstream)
		return
	}
	if lost {
		hold(ctx, b, "upstream_unfinished", "the upstream batch did not finish or return its results")
		return
	}
	status := nextStatus(b.Status, upstream.Status)
	updates := map[string]any{
		"status":            status,
		"request_completed": upstream.Completed,
		"request_failed":    upstream.Failed,
		"poll_failures":     0,
		"not_found_polls":   0,
		"next_poll_at":      now + pollInterval(),
	}
	if upstream.Total > 0 {
		updates["request_total"] = upstream.Total
	}
	if upstream.ExpiresAt > 0 {
		updates["expires_at"] = upstream.ExpiresAt
	}
	if status == model.BatchStatusInProgress && b.InProgressAt == 0 {
		updates["in_progress_at"] = now
	}
	if status == model.BatchStatusFinalizing && b.FinalizingAt == 0 {
		updates["finalizing_at"] = now
	}
	if _, err := model.UpdateBatchIfStatus(b.BatchId, b.Status, updates); err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to save the upstream state: %s", b.BatchId, err.Error()))
		delayPoll(ctx, b, now)
	}
}

var statusRank = map[string]int{
	model.BatchStatusValidating: 0,
	model.BatchStatusInProgress: 1,
	model.BatchStatusFinalizing: 2,
}

// nextStatus is the status an unfinished batch moves to for the upstream
// status: an unknown status keeps it, a terminal one without results yet
// shows as finalizing, a requested cancellation stays visible until the
// upstream ends the batch, and a batch never moves back.
func nextStatus(current string, upstream string) string {
	if current == model.BatchStatusCancelling {
		return current
	}
	if isTerminalStatus(upstream) {
		upstream = model.BatchStatusFinalizing
	}
	if upstream == model.BatchStatusCancelling {
		return upstream
	}
	rank, known := statusRank[upstream]
	if !known || rank < statusRank[current] {
		return current
	}
	return upstream
}

// holdUnreadable holds a batch whose upstream state cannot be read. The
// reason the user sees names no address; the error is logged for admins.
func holdUnreadable(ctx context.Context, b *model.Batch, code string, err error) {
	logger.LogError(ctx, common.LogText("batch %s (upstream %s, channel %d): %s", b.BatchId, b.UpstreamBatchId, b.ChannelId, err.Error()))
	hold(ctx, b, code, "the upstream state of the batch could not be read")
}

// hold stops polling a batch whose outcome cannot be determined and keeps its
// reservation until an administrator resumes, refunds or charges it.
func hold(ctx context.Context, b *model.Batch, code string, reason string) {
	holdWith(ctx, b, code, reason, nil)
}

// holdWith is hold, saving updates with the hold, and reports whether the
// hold was saved.
func holdWith(ctx context.Context, b *model.Batch, code string, reason string, updates map[string]any) bool {
	logger.LogError(ctx, common.LogText("batch %s (upstream %s, channel %d) is held for review with reservation %d: %s: %s",
		b.BatchId, b.UpstreamBatchId, b.ChannelId, b.Quota, code, reason))
	if updates == nil {
		updates = map[string]any{}
	}
	updates["errors"] = batchErrorsJSON(code, heldMessage(reason))
	held, err := model.HoldBatch(b.BatchId, updates)
	if err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to hold the batch: %s", b.BatchId, err.Error()))
		delayPoll(ctx, b, common.GetTimestamp())
	}
	return held
}

func heldMessage(reason string) string {
	return reason + "; the reservation is kept until an administrator settles the batch"
}

func batchSettlement(b *model.Batch, actual int) service.BatchSettlement {
	return service.BatchSettlement{
		BatchId:        b.BatchId,
		UserId:         b.UserId,
		TokenId:        b.TokenId,
		ChannelId:      b.ChannelId,
		Group:          b.UsingGroup,
		ModelName:      b.Model,
		BillingSource:  b.BillingSource,
		SubscriptionId: b.SubscriptionId,
		PreConsumed:    b.Quota,
		Actual:         actual,
		Uncounted:      b.SubmissionUnrecorded,
	}
}

// finalStatus is the status a batch ends with: a batch the user cancelled
// ends cancelled even when the upstream reports it completed.
func finalStatus(current string, upstream string) string {
	if current == model.BatchStatusCancelling && upstream == model.BatchStatusCompleted {
		return model.BatchStatusCancelled
	}
	return upstream
}

func finalize(ctx context.Context, b *model.Batch, upstream *UpstreamBatch) {
	var snapshot billingSnapshot
	if err := common.Unmarshal([]byte(b.BillingContext), &snapshot); err != nil {
		logger.LogError(ctx, common.LogText("batch %s: unreadable billing snapshot: %s", b.BatchId, err.Error()))
		hold(ctx, b, "billing_snapshot_invalid", "the batch price could not be read")
		return
	}
	var lines []*inputLine
	if input, err := model.GetBatchFile(b.UserId, b.InputFileId, true); err == nil {
		lines, _ = parseInput(input.Content, "", math.MaxInt32)
	} else {
		logger.LogWarn(ctx, common.LogText("batch %s: input file unavailable at settlement: %s", b.BatchId, err.Error()))
	}
	settlement := buildSettlement(b, &snapshot, lines, upstream)
	if settlement.Summary.Unexpected > 0 {
		logger.LogWarn(ctx, common.LogText("batch %s: the upstream returned %d results the input does not list; they are not charged",
			b.BatchId, settlement.Summary.Unexpected))
	}

	now := common.GetTimestamp()
	status := finalStatus(b.Status, upstream.Status)
	files := make([]*model.BatchFile, 0, 2)
	updates := map[string]any{
		"status":            status,
		"request_completed": settlement.Summary.Succeeded,
		"request_failed":    settlement.Summary.Failed + settlement.Summary.NotProcessed,
		"quota":             settlement.Summary.Quota,
		"usage_summary":     settlement.summaryJSON(),
		"poll_failures":     0,
		"not_found_polls":   0,
	}
	if b.RequestTotal == 0 {
		updates["request_total"] = len(settlement.Output) + len(settlement.Errors)
	}
	switch status {
	case model.BatchStatusCompleted:
		updates["completed_at"] = now
	case model.BatchStatusFailed:
		updates["failed_at"] = now
	case model.BatchStatusExpired:
		updates["expired_at"] = now
	case model.BatchStatusCancelled:
		updates["cancelled_at"] = now
	}
	if b.FinalizingAt == 0 {
		updates["finalizing_at"] = now
	}
	if upstream.ErrorMessage != "" {
		updates["errors"] = batchErrorsJSON("upstream_error", upstream.ErrorMessage)
	}
	retention := int64(operation_setting.GetBatchSetting().FileRetentionDays) * 86400
	for _, part := range []struct {
		content []byte
		column  string
		suffix  string
	}{{settlement.Output, "output_file_id", "output"}, {settlement.Errors, "error_file_id", "error"}} {
		if len(part.content) == 0 {
			continue
		}
		fileId, err := newObjectId("file-")
		if err != nil {
			backoff(ctx, b, now, 0, "failed to create a file id: "+err.Error())
			return
		}
		files = append(files, &model.BatchFile{
			FileId:    fileId,
			UserId:    b.UserId,
			TokenId:   b.TokenId,
			Purpose:   model.BatchFilePurposeOutput,
			Filename:  b.BatchId + "_" + part.suffix + ".jsonl",
			Bytes:     int64(len(part.content)),
			BatchId:   b.BatchId,
			Content:   part.content,
			CreatedAt: now,
			ExpiresAt: now + retention,
		})
		updates[part.column] = fileId
	}

	summary := settlement.Summary
	charge := batchSettlement(b, summary.Quota)
	won, applied, err := model.SettleBatch(b.BatchId, model.BatchBillingReserved, updates, files, service.BatchFunding(charge))
	if err != nil {
		// Each retry downloads the results again, so it backs off; a batch
		// that keeps failing to save is held rather than retried forever.
		if b.PollFailures+1 >= settleFailuresBeforeHold {
			logger.LogError(ctx, common.LogText("batch %s: failed to save the final state: %s", b.BatchId, err.Error()))
			hold(ctx, b, "settlement_failed", "the final state of the batch could not be saved")
			return
		}
		backoff(ctx, b, now, 0, "failed to save the final state: "+err.Error())
		return
	}
	if !won {
		return
	}
	other := model.NewLogOther()
	other.SetPublic("request_count", b.RequestTotal)
	other.SetPublic("succeeded", summary.Succeeded)
	other.SetPublic("failed", summary.Failed+summary.NotProcessed)
	other.SetPublic(batchRatioKey, snapshot.BatchRatio)
	noteTieredExpr(snapshot.Tiered, other)
	if len(summary.ModelVersions) > 0 {
		other.SetPublic("model_versions", summary.ModelVersions)
	}
	if summary.ResolvedModel != "" {
		other.SetPublic("resolved_model", summary.ResolvedModel)
	}
	if summary.EstimatedItems > 0 {
		other.SetPublic("estimated_items", summary.EstimatedItems)
	}
	if summary.GroundingQueries > 0 {
		other.SetPublic("grounding_queries", summary.GroundingQueries)
	}
	other.SetAdmin("upstream_batch_id", b.UpstreamBatchId)
	if summary.Unexpected > 0 {
		other.SetAdmin("unexpected_results", summary.Unexpected)
	}
	noteUpstreamCost(ctx, b, summary, other)
	noteQuotaSaturation(ctx, b, settlement.QuotaClamp, other)
	charge.PromptTokens = summary.PromptTokens
	charge.CompletionTokens = summary.CompletionTokens
	charge.Content = []*common.Message{common.NewMessage("Batch {{batch}} ended as {{status}}: {{succeeded}} succeeded, {{failed}} failed; charged from upstream usage",
		map[string]any{"batch": b.BatchId, "status": status, "succeeded": summary.Succeeded, "failed": summary.Failed + summary.NotProcessed})}
	charge.Other = other
	service.RecordBatchSettlement(ctx, charge, applied)
}

// noteUpstreamCost records the cost the upstream reports and warns when it
// exceeds the charge, which the token prices then do not cover.
func noteUpstreamCost(ctx context.Context, b *model.Batch, summary usageSummary, other *model.LogOther) {
	if summary.UpstreamCostUSD == nil {
		return
	}
	other.SetAdmin("upstream_cost_usd", *summary.UpstreamCostUSD)
	costQuota := *summary.UpstreamCostUSD * common.QuotaPerUnit
	if costQuota > float64(summary.Quota) {
		other.SetAdmin("upstream_cost_exceeds_charge", true)
		logger.LogWarn(ctx, common.LogText("batch %s: upstream cost $%.6f (%.0f quota) exceeds the charge %d; check the model price",
			b.BatchId, *summary.UpstreamCostUSD, costQuota, summary.Quota))
	}
}

// ---------------------------------------------------------------------------
// Cancel and delete
// ---------------------------------------------------------------------------

func CancelBatch(c *gin.Context, batchId string) (*model.Batch, *Error) {
	userId := c.GetInt("id")
	b, err := model.GetBatch(userId, batchId)
	if err != nil {
		return nil, notFoundOr(err, i18n.MsgBatchNotFound)
	}
	if b.Status == model.BatchStatusCancelling {
		return b, nil
	}
	if isTerminalStatus(b.Status) || b.UpstreamBatchId == "" {
		return nil, newError(http.StatusConflict, "invalid_request_error", i18n.MsgBatchCannotCancel, map[string]any{"Status": b.Status})
	}
	conn, adaptor, err := resolveConnection(b)
	if err != nil {
		return nil, internalError(err)
	}
	// Cancelling a batch the upstream just finished can drop its results.
	retrieveCtx, cancelRetrieve := context.WithTimeout(c.Request.Context(), cancelCheckTimeout)
	upstream, err := adaptor.Retrieve(retrieveCtx, conn, b.UpstreamBatchId)
	cancelRetrieve()
	if err == nil && isTerminalStatus(upstream.Status) {
		_, _ = model.UpdateBatchIfStatus(b.BatchId, b.Status, map[string]any{"next_poll_at": common.GetTimestamp()})
		return nil, newError(http.StatusConflict, "invalid_request_error", i18n.MsgBatchCannotCancel, map[string]any{"Status": upstream.Status})
	}
	if _, err := adaptor.Cancel(c.Request.Context(), conn, b.UpstreamBatchId); err != nil {
		if errors.Is(err, ErrCancelNotSupported) {
			return nil, newError(http.StatusBadRequest, "cancel_not_supported", i18n.MsgBatchCancelNotSupported, nil)
		}
		return nil, submitError(err)
	}
	now := common.GetTimestamp()
	if _, err := model.UpdateBatchIfStatus(b.BatchId, b.Status, map[string]any{
		"status":        model.BatchStatusCancelling,
		"cancelling_at": now,
		"next_poll_at":  now,
	}); err != nil {
		return nil, internalError(err)
	}
	fresh, err := model.GetBatch(userId, batchId)
	if err != nil {
		return nil, internalError(err)
	}
	return fresh, nil
}

// DeleteBatch removes a finished and settled batch here and upstream. Its
// output files stay until they expire or are deleted.
func DeleteBatch(c *gin.Context, batchId string) *Error {
	userId := c.GetInt("id")
	b, err := model.GetBatch(userId, batchId)
	if err != nil {
		return notFoundOr(err, i18n.MsgBatchNotFound)
	}
	if !isTerminalStatus(b.Status) || b.BillingState != model.BatchBillingSettled {
		return newError(http.StatusConflict, "invalid_request_error", i18n.MsgBatchNotDeletable, map[string]any{"Status": b.Status})
	}
	if b.UpstreamBatchId != "" {
		if conn, adaptor, err := resolveConnection(b); err == nil {
			if err := adaptor.Delete(c.Request.Context(), conn, b.UpstreamBatchId); err != nil && !IsUpstreamNotFound(err) {
				return submitError(err)
			}
		}
	}
	if _, err := model.DeleteSettledBatch(userId, batchId); err != nil {
		return internalError(err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Held batches
// ---------------------------------------------------------------------------

const (
	ResolveActionResume = "resume"
	ResolveActionRefund = "refund"
	ResolveActionCharge = "charge"
)

type ResolveRequest struct {
	Action string `json:"action"`
	// Quota is the charge of a charge action; it defaults to the reservation.
	Quota *int   `json:"quota"`
	Note  string `json:"note"`
}

// HeldBatch is a held batch as an administrator sees it.
type HeldBatch struct {
	BatchObject
	UserId          int    `json:"user_id"`
	ChannelId       int    `json:"channel_id"`
	UpstreamBatchId string `json:"upstream_batch_id"`
	ReservedQuota   int    `json:"reserved_quota"`
	BillingSource   string `json:"billing_source"`
}

func newHeldBatch(b *model.Batch) HeldBatch {
	return HeldBatch{
		BatchObject:     NewBatchObject(b),
		UserId:          b.UserId,
		ChannelId:       b.ChannelId,
		UpstreamBatchId: b.UpstreamBatchId,
		ReservedQuota:   b.Quota,
		BillingSource:   b.BillingSource,
	}
}

func ListHeldBatches(limit int) ([]HeldBatch, *Error) {
	batches, err := model.ListHeldBatches(limit)
	if err != nil {
		return nil, internalError(err)
	}
	held := make([]HeldBatch, 0, len(batches))
	for _, b := range batches {
		held = append(held, newHeldBatch(b))
	}
	return held, nil
}

// ResolveHeldBatch ends the hold of a batch: resume polls it again, refund
// returns its reservation and charge settles it at a set quota. adminId is
// recorded in the log.
func ResolveHeldBatch(ctx context.Context, adminId int, batchId string, req *ResolveRequest) (*HeldBatch, *Error) {
	b, err := model.GetBatchByBatchId(batchId)
	if err != nil {
		return nil, notFoundOr(err, i18n.MsgBatchNotFound)
	}
	if b.BillingState != model.BatchBillingHeld {
		return nil, newError(http.StatusConflict, "invalid_request_error", i18n.MsgBatchNotHeld, nil)
	}
	now := common.GetTimestamp()
	switch req.Action {
	case ResolveActionResume:
		if _, err := model.ResumeHeldBatch(batchId, map[string]any{
			"poll_failures":   0,
			"not_found_polls": 0,
			"next_poll_at":    now,
			"errors":          "",
		}); err != nil {
			return nil, internalError(err)
		}
	case ResolveActionRefund, ResolveActionCharge:
		quota := 0
		if req.Action == ResolveActionCharge {
			quota = b.Quota
			if req.Quota != nil {
				quota = *req.Quota
			}
			if quota < 0 {
				return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidResolution, nil)
			}
		}
		status := b.Status
		updates := map[string]any{"quota": quota, "poll_failures": 0}
		if !isTerminalStatus(status) {
			status = model.BatchStatusFailed
			updates["status"] = status
			updates["failed_at"] = now
		}
		reason := "settled by an administrator"
		if req.Note != "" {
			reason += ": " + req.Note
		}
		updates["errors"] = batchErrorsJSON("resolved_by_admin", reason)
		charge := batchSettlement(b, quota)
		won, applied, err := model.SettleBatch(batchId, model.BatchBillingHeld, updates, nil, service.BatchFunding(charge))
		if err != nil {
			return nil, internalError(err)
		}
		if won {
			other := model.NewLogOther()
			other.SetAdmin("upstream_batch_id", b.UpstreamBatchId)
			other.SetAdmin("resolved_by", adminId)
			if req.Note != "" {
				other.SetAdmin("resolution_note", req.Note)
			}
			charge.Content = []*common.Message{common.NewMessage("Batch {{batch}} was settled by an administrator after its upstream outcome could not be read",
				map[string]any{"batch": b.BatchId})}
			charge.Other = other
			service.RecordBatchSettlement(ctx, charge, applied)
		}
	default:
		return nil, newError(http.StatusBadRequest, "invalid_request_error", i18n.MsgBatchInvalidResolution, nil)
	}
	fresh, err := model.GetBatchByBatchId(batchId)
	if err != nil {
		return nil, internalError(err)
	}
	held := newHeldBatch(fresh)
	return &held, nil
}

func ListBatches(userId int, after string, limit int) (ListObject[BatchObject], *Error) {
	batches, hasMore, err := model.ListBatches(userId, after, limit)
	if err != nil {
		return ListObject[BatchObject]{}, notFoundOr(err, i18n.MsgBatchNotFound)
	}
	list := ListObject[BatchObject]{Object: "list", Data: make([]BatchObject, 0, len(batches)), HasMore: hasMore}
	for _, b := range batches {
		list.Data = append(list.Data, NewBatchObject(b))
	}
	if len(batches) > 0 {
		list.FirstId = &batches[0].BatchId
		list.LastId = &batches[len(batches)-1].BatchId
	}
	return list, nil
}

func ListFiles(userId int, purpose string, after string, limit int) (ListObject[FileObject], *Error) {
	files, hasMore, err := model.ListBatchFiles(userId, purpose, after, limit)
	if err != nil {
		return ListObject[FileObject]{}, notFoundOr(err, i18n.MsgBatchFileNotFound)
	}
	list := ListObject[FileObject]{Object: "list", Data: make([]FileObject, 0, len(files)), HasMore: hasMore}
	for _, file := range files {
		list.Data = append(list.Data, NewFileObject(file))
	}
	if len(files) > 0 {
		list.FirstId = &files[0].FileId
		list.LastId = &files[len(files)-1].FileId
	}
	return list, nil
}

func trimFilename(name string) string {
	name = strings.TrimSpace(name)
	if len(name) > 255 {
		name = name[:255]
	}
	return name
}
