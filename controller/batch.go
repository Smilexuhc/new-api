package controller

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay/batch"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
)

const (
	defaultBatchListLimit = 20
	maxBatchListLimit     = 100
)

// BatchGate answers /v1/batches like an unregistered route while the batch
// API is disabled, before authentication runs.
func BatchGate() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !operation_setting.GetBatchSetting().Enabled {
			RelayNotFound(c)
			c.Abort()
			return
		}
		c.Next()
	}
}

// BatchFilesGate serves /v1/files from the batch file store while the batch
// API is enabled. Disabled, the request continues to the existing handlers.
func BatchFilesGate() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !operation_setting.GetBatchSetting().Enabled {
			c.Next()
			return
		}
		switch c.Request.Method + " " + c.FullPath() {
		case http.MethodPost + " /v1/files":
			UploadBatchFile(c)
		case http.MethodGet + " /v1/files":
			ListBatchFiles(c)
		case http.MethodDelete + " /v1/files/:id":
			DeleteBatchFile(c)
		case http.MethodGet + " /v1/files/:id/content":
			DownloadBatchFile(c)
		default:
			c.Next()
			return
		}
		c.Abort()
	}
}

func writeBatchError(c *gin.Context, apiErr *batch.Error) {
	errorType := "invalid_request_error"
	if apiErr.Status >= http.StatusInternalServerError {
		errorType = "server_error"
	}
	c.JSON(apiErr.Status, gin.H{"error": types.OpenAIError{
		Message: apiErr.Text(c),
		Type:    errorType,
		Code:    apiErr.Code,
	}})
}

func batchListLimit(c *gin.Context) int {
	limit, err := strconv.Atoi(c.Query("limit"))
	if err != nil || limit <= 0 {
		return defaultBatchListLimit
	}
	return min(limit, maxBatchListLimit)
}

func UploadBatchFile(c *gin.Context) {
	header, err := c.FormFile("file")
	purpose := c.PostForm("purpose")
	if err != nil || purpose == "" {
		writeBatchError(c, &batch.Error{Status: http.StatusBadRequest, Code: "invalid_request_error", MsgID: i18n.MsgBatchFileRequired})
		return
	}
	maxBytes := int64(operation_setting.GetBatchSetting().MaxFileSizeMB) << 20
	if header.Size > maxBytes {
		writeBatchError(c, &batch.Error{Status: http.StatusRequestEntityTooLarge, Code: "invalid_request_error", MsgID: i18n.MsgBatchFileTooLarge,
			Params: map[string]any{"MaxMB": operation_setting.GetBatchSetting().MaxFileSizeMB}})
		return
	}
	file, err := header.Open()
	if err != nil {
		writeBatchError(c, &batch.Error{Status: http.StatusBadRequest, Code: "invalid_request_error", MsgID: i18n.MsgBatchFileRequired})
		return
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		writeBatchError(c, &batch.Error{Status: http.StatusBadRequest, Code: "invalid_request_error", MsgID: i18n.MsgBatchFileRequired})
		return
	}
	stored, apiErr := batch.UploadFile(c.GetInt("id"), c.GetInt("token_id"), purpose, header.Filename, content)
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, batch.NewFileObject(stored))
}

func ListBatchFiles(c *gin.Context) {
	list, apiErr := batch.ListFiles(c.GetInt("id"), c.Query("purpose"), c.Query("after"), batchListLimit(c))
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, list)
}

func DeleteBatchFile(c *gin.Context) {
	fileId := c.Param("id")
	if apiErr := batch.DeleteFile(c.GetInt("id"), fileId); apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": fileId, "object": "file", "deleted": true})
}

func DownloadBatchFile(c *gin.Context) {
	file, apiErr := batch.GetFileContent(c.GetInt("id"), c.Param("id"))
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.Data(http.StatusOK, "application/jsonl", file.Content)
}

// RetrieveBatchFile is OpenAI files.retrieve: the file object, without its bytes.
func RetrieveBatchFile(c *gin.Context) {
	file, apiErr := batch.GetFile(c.GetInt("id"), c.Param("id"))
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, batch.NewFileObject(file))
}

func CreateBatch(c *gin.Context) {
	var req batch.CreateRequest
	if err := common.UnmarshalBodyReusable(c, &req); err != nil {
		writeBatchError(c, &batch.Error{Status: http.StatusBadRequest, Code: "invalid_request_error", MsgID: i18n.MsgBatchInvalidRequest,
			Params: map[string]any{"Error": err.Error()}})
		return
	}
	created, apiErr := batch.CreateBatch(c, &req)
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, batch.NewBatchObject(created))
}

func RetrieveBatch(c *gin.Context) {
	b, err := model.GetBatch(c.GetInt("id"), c.Param("id"))
	if err != nil {
		writeBatchError(c, &batch.Error{Status: http.StatusNotFound, Code: "not_found", MsgID: i18n.MsgBatchNotFound})
		return
	}
	c.JSON(http.StatusOK, batch.NewBatchObject(b))
}

func ListBatches(c *gin.Context) {
	list, apiErr := batch.ListBatches(c.GetInt("id"), c.Query("after"), batchListLimit(c))
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, list)
}

func CancelBatch(c *gin.Context) {
	b, apiErr := batch.CancelBatch(c, c.Param("id"))
	if apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, batch.NewBatchObject(b))
}

func DeleteBatch(c *gin.Context) {
	batchId := c.Param("id")
	if apiErr := batch.DeleteBatch(c, batchId); apiErr != nil {
		writeBatchError(c, apiErr)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": batchId, "object": "batch", "deleted": true})
}

// AdminListHeldBatches lists the batches whose upstream outcome could not be
// read and that wait for an administrator.
func AdminListHeldBatches(c *gin.Context) {
	held, apiErr := batch.ListHeldBatches(batchListLimit(c))
	if apiErr != nil {
		common.ApiErrorMsg(c, apiErr.Text(c))
		return
	}
	common.ApiSuccess(c, held)
}

// AdminResolveHeldBatch resumes, refunds or charges a held batch.
func AdminResolveHeldBatch(c *gin.Context) {
	var req batch.ResolveRequest
	if err := common.UnmarshalBodyReusable(c, &req); err != nil {
		common.ApiErrorStatus(c, http.StatusBadRequest, err)
		return
	}
	resolved, apiErr := batch.ResolveHeldBatch(c, c.GetInt("id"), c.Param("id"), &req)
	if apiErr != nil {
		// The console envelope stays {success, message}; the status is the one
		// the service decided (409 already settled, 400 illegal action).
		common.ApiErrorStatus(c, apiErr.Status, errors.New(apiErr.Text(c)))
		return
	}
	common.ApiSuccess(c, resolved)
}
