package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchRoutesDoNotShadowPluginRoutes(t *testing.T) {
	setupRelayRouterTestDB(t)
	engine := gin.New()
	SetRelayRouter(engine)
	builder := newPluginGenerationBuilder(engine.Routes(), nil, productionPluginRouteHandlers)
	for _, route := range []jsplugin.Route{
		{Method: http.MethodPost, Path: "/v1/batches"},
		{Method: http.MethodGet, Path: "/v1/batches"},
		{Method: http.MethodGet, Path: "/v1/batches/:id"},
		{Method: http.MethodPost, Path: "/v1/batches/:id/cancel"},
		{Method: http.MethodDelete, Path: "/v1/batches/:id"},
	} {
		assert.False(t, builder.routeAnsweredByHost(route), "%s %s", route.Method, route.Path)
	}
}

func TestDisabledBatchRoutesAnswerLikeUnknownRoutes(t *testing.T) {
	setupRelayRouterTestDB(t)
	require.NoError(t, i18n.Init())
	setting := operation_setting.GetBatchSetting()
	enabled := setting.Enabled
	setting.Enabled = false
	t.Cleanup(func() { setting.Enabled = enabled })

	engine := gin.New()
	SetRelayRouter(engine)
	engine.NoRoute(controller.RelayNotFound)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/v1/batches"},
		{http.MethodGet, "/v1/batches/"},
		{http.MethodPost, "/v1/batches/"},
		{http.MethodGet, "/v1/batches/b1/"},
		{http.MethodPost, "/v1/batches/b1/cancel/"},
		{http.MethodGet, "/v1/files/file_sample"},
		{http.MethodGet, "/v1/files/file_sample/"},
	} {
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
		assert.Equal(t, http.StatusNotFound, recorder.Code, "%s %s", tc.method, tc.path)
		assert.Empty(t, recorder.Header().Get("Location"), "%s %s", tc.method, tc.path)
	}
}

func TestEnabledFileRetrieveRequiresAToken(t *testing.T) {
	setupRelayRouterTestDB(t)
	require.NoError(t, i18n.Init())
	require.NoError(t, model.DB.AutoMigrate(&model.BatchFile{}))
	setting := operation_setting.GetBatchSetting()
	enabled := setting.Enabled
	setting.Enabled = true
	t.Cleanup(func() { setting.Enabled = enabled })

	user := model.User{Username: "file-owner", Status: common.UserStatusEnabled, Group: "default", Quota: 1000, AffCode: "file-aff"}
	require.NoError(t, model.DB.Create(&user).Error)
	token := model.Token{UserId: user.Id, Key: "fileretrievetoken", Name: "t", Status: common.TokenStatusEnabled, ExpiredTime: -1, RemainQuota: 1000, Group: "default"}
	require.NoError(t, model.DB.Create(&token).Error)
	require.NoError(t, model.DB.Create(&model.BatchFile{
		FileId: "file-abc", UserId: user.Id, TokenId: token.Id, Purpose: model.BatchFilePurposeInput,
		Filename: "in.jsonl", Bytes: 12, Content: []byte("SECRET-BYTES"), CreatedAt: 10,
	}).Error)

	engine := gin.New()
	SetRelayRouter(engine)
	anonymous := httptest.NewRecorder()
	engine.ServeHTTP(anonymous, httptest.NewRequest(http.MethodGet, "/v1/files/file-abc", nil))
	assert.Equal(t, http.StatusUnauthorized, anonymous.Code)
	assert.NotContains(t, anonymous.Body.String(), "API not implemented")

	request := httptest.NewRequest(http.MethodGet, "/v1/files/file-abc", nil)
	request.Header.Set("Authorization", "Bearer sk-fileretrievetoken")
	retrieved := httptest.NewRecorder()
	engine.ServeHTTP(retrieved, request)
	assert.Equal(t, http.StatusOK, retrieved.Code, retrieved.Body.String())
	assert.Contains(t, retrieved.Body.String(), `"id":"file-abc"`)
	assert.Contains(t, retrieved.Body.String(), `"object":"file"`)
	assert.NotContains(t, retrieved.Body.String(), "SECRET-BYTES")

	missing := httptest.NewRecorder()
	missingRequest := httptest.NewRequest(http.MethodGet, "/v1/files/file-missing", nil)
	missingRequest.Header.Set("Authorization", "Bearer sk-fileretrievetoken")
	engine.ServeHTTP(missing, missingRequest)
	assert.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
}

func TestAdminResolveHeldBatchKeepsTheErrorStatus(t *testing.T) {
	setupRelayRouterTestDB(t)
	require.NoError(t, i18n.Init())
	require.NoError(t, model.DB.AutoMigrate(&model.Batch{}))
	require.NoError(t, model.DB.Create(&model.User{
		Id: 1, Username: "admin", Quota: 1000, Status: common.UserStatusEnabled, Group: "default", AffCode: "aff",
	}).Error)
	require.NoError(t, model.DB.Create(&model.Batch{
		BatchId: "batch_held", UserId: 1, Status: model.BatchStatusValidating,
		BillingState: model.BatchBillingHeld, Quota: 100, Endpoint: "/v1/chat/completions",
		Model: "m", InputFileId: "file_x", CompletionWindow: "24h", CreatedAt: 1,
	}).Error)

	post := func(body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/api/batch/admin/held/batch_held/resolve", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Params = gin.Params{{Key: "id", Value: "batch_held"}}
		c.Set("id", 1)
		controller.AdminResolveHeldBatch(c)
		return recorder
	}

	broken := post("{")
	assert.Equal(t, http.StatusBadRequest, broken.Code)
	assert.Contains(t, broken.Body.String(), `"success":false`)

	negative := post(`{"action":"charge","quota":-1}`)
	assert.Equal(t, http.StatusBadRequest, negative.Code)
	assert.Contains(t, negative.Body.String(), `"success":false`)

	illegal := post(`{"action":"nope"}`)
	assert.Equal(t, http.StatusBadRequest, illegal.Code)

	resumed := post(`{"action":"resume"}`)
	assert.Equal(t, http.StatusOK, resumed.Code, resumed.Body.String())
	assert.Contains(t, resumed.Body.String(), `"success":true`)

	again := post(`{"action":"resume"}`)
	assert.Equal(t, http.StatusConflict, again.Code, again.Body.String())
	assert.Contains(t, again.Body.String(), `"success":false`)
}
