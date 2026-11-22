package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"strings"
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
	// upstreamLostAfter is how long past its expiry an unfinished batch keeps
	// being polled before it is given up.
	upstreamLostAfter = 7 * 24 * time.Hour
	maxPollBackoff    = 30 * time.Minute
	pollBatchSize     = 100
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
	fileId, err := newObjectId("file-")
	if err != nil {
		return nil, internalError(err)
	}
	now := common.GetTimestamp()
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

	items := make([]SubmitItem, 0, len(lines))
	converter, converts := adaptor.(RequestConverter)
	for _, line := range lines {
		body, err := line.withModel(info.UpstreamModelName)
		if err != nil {
			return nil, lineError(line.Line, err.Error())
		}
		if converts {
			var request dto.GeneralOpenAIRequest
			if err := common.Unmarshal(body, &request); err != nil {
				return nil, lineError(line.Line, err.Error())
			}
			if body, err = converter.ConvertRequest(c, info, &request); err != nil {
				return nil, lineError(line.Line, err.Error())
			}
		}
		items = append(items, SubmitItem{CustomId: line.CustomId, Body: body})
	}

	_, preConsume, clamp, err := estimateCharges(c, snapshot, lines)
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
	}
	// The row exists before the upstream call so a crash after submission
	// leaves a record that the poller settles instead of a lost reservation.
	if err := model.CreateBatch(b); err != nil {
		refund()
		return nil, internalError(err)
	}

	conn := &Connection{
		ChannelType: channel.Type,
		BaseURL:     channelBaseURL(channel),
		Key:         key,
		Proxy:       channel.GetSetting().Proxy,
	}
	upstream, err := adaptor.Submit(c.Request.Context(), conn, &SubmitRequest{
		BatchId:          batchId,
		Endpoint:         req.Endpoint,
		UpstreamModel:    info.UpstreamModelName,
		CompletionWindow: req.CompletionWindow,
		Items:            items,
	})
	if err != nil {
		refund()
		if deleteErr := model.DeleteUnsubmittedBatch(batchId); deleteErr != nil {
			logger.LogError(c, common.LogText("batch %s: failed to delete the record of a rejected submission: %s", batchId, deleteErr.Error()))
		}
		return nil, submitError(err)
	}

	if err := service.SettleBilling(c, info, preConsume); err != nil {
		logger.LogError(c, common.LogText("batch %s: failed to confirm the reservation: %s", batchId, err.Error()))
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
	if _, err := model.UpdateBatchIfStatus(batchId, model.BatchStatusValidating, updates); err != nil {
		logger.LogError(c, common.LogText("batch %s: submitted as %s but the record update failed: %s", batchId, upstream.Id, err.Error()))
	}
	recordSubmitLog(c, info, b, ratio, ratioSource, clamp)
	model.UpdateUserUsedQuotaAndRequestCount(userId, preConsume)
	model.UpdateChannelUsedQuota(channel.Id, preConsume)

	fresh, err := model.GetBatch(userId, batchId)
	if err != nil {
		return b, nil
	}
	return fresh, nil
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

func resolveConnection(b *model.Batch) (*Connection, Adaptor, error) {
	adaptor := GetAdaptor(b.ChannelType)
	if adaptor == nil {
		return nil, nil, errors.New("no batch adaptor for the channel type")
	}
	channel, err := model.GetChannelById(b.ChannelId, true)
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
	return nil, nil, errors.New("the channel key that submitted the batch is gone")
}

// PollOnce refreshes the batches whose poll time has come. It returns the
// number of batches it looked at.
func PollOnce(ctx context.Context) int {
	now := common.GetTimestamp()
	batches, err := model.FindDueBatches(now, pollBatchSize)
	if err != nil {
		logger.LogError(ctx, common.LogText("batch poll: failed to load due batches: %s", err.Error()))
		return 0
	}
	for _, b := range batches {
		refreshBatch(ctx, b, now)
	}
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

func backoff(ctx context.Context, b *model.Batch, now int64, reason string) {
	failures := b.PollFailures + 1
	delay := pollInterval() << min(failures, 10)
	delay = min(delay, int64(maxPollBackoff.Seconds()))
	logger.LogWarn(ctx, common.LogText("batch %s poll failed (%d in a row), next try in %ds: %s", b.BatchId, failures, delay, reason))
	if _, err := model.UpdateBatchIfStatus(b.BatchId, b.Status, map[string]any{
		"poll_failures": failures,
		"next_poll_at":  now + delay,
	}); err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to record the poll failure: %s", b.BatchId, err.Error()))
	}
}

func refreshBatch(ctx context.Context, b *model.Batch, now int64) {
	if b.UpstreamBatchId == "" {
		if now-b.CreatedAt >= int64(submitGracePeriod.Seconds()) {
			giveUp(ctx, b, "submission_lost", "the submission did not complete", 0)
		}
		return
	}
	lost := b.ExpiresAt > 0 && now > b.ExpiresAt+int64(upstreamLostAfter.Seconds())
	conn, adaptor, err := resolveConnection(b)
	if err != nil {
		if lost {
			giveUp(ctx, b, "upstream_unreachable", err.Error(), b.Quota)
			return
		}
		backoff(ctx, b, now, err.Error())
		return
	}
	upstream, err := adaptor.Retrieve(ctx, conn, b.UpstreamBatchId)
	if err != nil {
		if IsUpstreamNotFound(err) || lost {
			giveUp(ctx, b, "upstream_unavailable", err.Error(), b.Quota)
			return
		}
		backoff(ctx, b, now, err.Error())
		return
	}
	if isTerminalStatus(upstream.Status) && upstream.ResultsReady {
		finalize(ctx, b, upstream)
		return
	}
	if lost {
		giveUp(ctx, b, "upstream_unavailable", "the upstream batch did not finish", b.Quota)
		return
	}
	status := upstream.Status
	if isTerminalStatus(status) {
		status = model.BatchStatusFinalizing
	}
	// A requested cancellation stays visible until the upstream ends the batch.
	if b.Status == model.BatchStatusCancelling {
		status = model.BatchStatusCancelling
	}
	updates := map[string]any{
		"status":            status,
		"request_completed": upstream.Completed,
		"request_failed":    upstream.Failed,
		"poll_failures":     0,
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
	}
}

// giveUp ends a batch whose upstream state can no longer be read. quota is
// what it is charged: nothing when it never reached the upstream, the
// reservation when the upstream may have billed it.
func giveUp(ctx context.Context, b *model.Batch, code string, reason string, quota int) {
	logger.LogError(ctx, common.LogText("batch %s (upstream %s, channel %d) is given up and marked failed with charge %d of reserved %d: %s",
		b.BatchId, b.UpstreamBatchId, b.ChannelId, quota, b.Quota, reason))
	now := common.GetTimestamp()
	updates := map[string]any{
		"status":    model.BatchStatusFailed,
		"failed_at": now,
		"errors":    batchErrorsJSON(code, reason),
		"quota":     quota,
	}
	won, err := model.SettleBatch(b.BatchId, updates, nil)
	if err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to mark the batch failed: %s", b.BatchId, err.Error()))
		return
	}
	if !won {
		return
	}
	other := model.NewLogOther()
	other.SetAdmin("upstream_batch_id", b.UpstreamBatchId)
	other.SetAdmin("reason", reason)
	settle(ctx, b, quota, 0, 0, []*common.Message{common.NewMessage("Batch {{batch}} failed because its upstream state could not be read ({{code}})",
		map[string]any{"batch": b.BatchId, "code": code})}, other)
}

func settle(ctx context.Context, b *model.Batch, actual int, promptTokens int, completionTokens int, content []*common.Message, other *model.LogOther) {
	if err := service.SettleBatchCharge(ctx, service.BatchSettlement{
		BatchId:          b.BatchId,
		UserId:           b.UserId,
		TokenId:          b.TokenId,
		ChannelId:        b.ChannelId,
		Group:            b.UsingGroup,
		ModelName:        b.Model,
		BillingSource:    b.BillingSource,
		SubscriptionId:   b.SubscriptionId,
		PreConsumed:      b.Quota,
		Actual:           actual,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		Content:          content,
		Other:            other,
	}); err != nil {
		logger.LogError(ctx, common.LogText("batch %s is settled in the database but its funds were not adjusted (actual %d, reserved %d); reconcile by hand: %s",
			b.BatchId, actual, b.Quota, err.Error()))
	}
}

func finalize(ctx context.Context, b *model.Batch, upstream *UpstreamBatch) {
	var snapshot billingSnapshot
	if err := common.Unmarshal([]byte(b.BillingContext), &snapshot); err != nil {
		logger.LogError(ctx, common.LogText("batch %s: unreadable billing snapshot, keeping the reservation: %s", b.BatchId, err.Error()))
		giveUp(ctx, b, "billing_snapshot_invalid", err.Error(), b.Quota)
		return
	}
	var lines []*inputLine
	if input, err := model.GetBatchFile(b.UserId, b.InputFileId, true); err == nil {
		lines, _ = parseInput(input.Content, "", math.MaxInt32)
	} else {
		logger.LogWarn(ctx, common.LogText("batch %s: input file unavailable at settlement: %s", b.BatchId, err.Error()))
	}
	settlement := buildSettlement(b, &snapshot, lines, upstream)

	now := common.GetTimestamp()
	files := make([]*model.BatchFile, 0, 2)
	updates := map[string]any{
		"status":            upstream.Status,
		"request_completed": settlement.Summary.Succeeded,
		"request_failed":    settlement.Summary.Failed + settlement.Summary.NotProcessed,
		"quota":             settlement.Summary.Quota,
		"usage_summary":     settlement.summaryJSON(),
		"poll_failures":     0,
	}
	if b.RequestTotal == 0 {
		updates["request_total"] = len(settlement.Output) + len(settlement.Errors)
	}
	switch upstream.Status {
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
			logger.LogError(ctx, common.LogText("batch %s: failed to create a file id: %s", b.BatchId, err.Error()))
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

	won, err := model.SettleBatch(b.BatchId, updates, files)
	if err != nil {
		logger.LogError(ctx, common.LogText("batch %s: failed to save the final state: %s", b.BatchId, err.Error()))
		return
	}
	if !won {
		return
	}
	summary := settlement.Summary
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
	other.SetAdmin("upstream_batch_id", b.UpstreamBatchId)
	if summary.UpstreamCostUSD != nil {
		other.SetAdmin("upstream_cost_usd", *summary.UpstreamCostUSD)
	}
	noteQuotaSaturation(ctx, b, settlement.QuotaClamp, other)
	settle(ctx, b, summary.Quota, summary.PromptTokens, summary.CompletionTokens,
		[]*common.Message{common.NewMessage("Batch {{batch}} ended as {{status}}: {{succeeded}} succeeded, {{failed}} failed; charged from upstream usage",
			map[string]any{"batch": b.BatchId, "status": upstream.Status, "succeeded": summary.Succeeded, "failed": summary.Failed + summary.NotProcessed})},
		other)
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
