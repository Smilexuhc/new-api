package batch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/setting/billing_setting"
	"github.com/QuantumNous/new-api/setting/config"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	testModel     = "batch-test-model"
	testUser      = 1
	otherUser     = 2
	testToken     = 11
	otherToken    = 12
	testTokenKey  = "batch-test-token"
	otherTokenKey = "batch-other-token"
	startQuota    = 10_000_000
)

// TestMain uses an in-memory SQLite database. Set BATCH_TEST_SQL_DSN to a
// MySQL or PostgreSQL DSN to run the suite through the production migration
// on that database instead.
func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	common.RedisEnabled = false
	common.BatchUpdateEnabled = false
	common.LogConsumeEnabled = true
	common.MemoryCacheEnabled = true
	if dsn := os.Getenv("BATCH_TEST_SQL_DSN"); dsn != "" {
		os.Setenv("SQL_DSN", dsn)
		common.IsMasterNode = true
		if err := model.InitDB(); err != nil {
			panic(err)
		}
		if err := model.InitLogDB(); err != nil {
			panic(err)
		}
	} else {
		openSQLiteTestDB()
	}
	if err := i18n.Init(); err != nil {
		panic(err)
	}
	if err := ratio_setting.UpdateModelRatioByJSONString(`{"` + testModel + `":1}`); err != nil {
		panic(err)
	}
	if err := ratio_setting.UpdateCompletionRatioByJSONString(`{"` + testModel + `":2}`); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func openSQLiteTestDB() {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxOpenConns(1)
	model.DB = db
	model.LOG_DB = db
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
	if err := db.AutoMigrate(
		&model.User{}, &model.Token{}, &model.Log{}, &model.Channel{}, &model.Ability{},
		&model.SystemTask{}, &model.SystemTaskLock{},
		&model.UserSubscription{}, &model.SubscriptionPreConsumeRecord{}, &model.SubscriptionPlan{},
		&model.Batch{}, &model.BatchFile{},
	); err != nil {
		panic(err)
	}
}

// ---------------------------------------------------------------------------
// Fake upstream serving the OpenRouter and Gemini batch APIs
// ---------------------------------------------------------------------------

type fakeUpstream struct {
	mu           sync.Mutex
	server       *httptest.Server
	submits      int
	lastSubmit   []byte
	submitStatus int
	retrieve     string
	retrieveCode int
	cancels      int
	deletes      int
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	f := &fakeUpstream{}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeUpstream) set(retrieve string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retrieve = retrieve
	f.retrieveCode = 0
}

func (f *fakeUpstream) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && (path == "/v1/batches" || strings.HasSuffix(path, ":batchGenerateContent")):
		f.submits++
		f.lastSubmit = body
		if f.submitStatus != 0 {
			w.WriteHeader(f.submitStatus)
			_, _ = w.Write([]byte(`{"error":{"message":"upstream rejected the batch"}}`))
			return
		}
		if path == "/v1/batches" {
			_, _ = fmt.Fprintf(w, `{"id":"orb-1","status":"validating","expires_at":%d,"request_counts":{"total":2,"completed":0,"failed":0},"results":null}`, time.Now().Add(24*time.Hour).Unix())
			return
		}
		_, _ = w.Write([]byte(`{"name":"batches/g1","metadata":{"state":"BATCH_STATE_PENDING","batchStats":{"requestCount":"2"}}}`))
	case r.Method == http.MethodPost && strings.HasSuffix(path, ":cancel"):
		f.cancels++
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodGet:
		if f.retrieveCode != 0 {
			w.WriteHeader(f.retrieveCode)
			_, _ = w.Write([]byte(`{"error":{"message":"not found"}}`))
			return
		}
		_, _ = w.Write([]byte(f.retrieve))
	case r.Method == http.MethodDelete:
		f.deletes++
		_, _ = w.Write([]byte(`{}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func resetDB(t *testing.T) {
	t.Helper()
	for _, table := range []string{"users", "tokens", "logs", "channels", "abilities", "user_subscriptions",
		"subscription_pre_consume_records", "batches", "batch_files", "system_tasks"} {
		require.NoError(t, model.DB.Exec("DELETE FROM "+table).Error)
	}
	setting := operation_setting.GetBatchSetting()
	*setting = operation_setting.BatchSetting{
		Enabled:                true,
		MaxRequestsPerBatch:    100,
		MaxFileSizeMB:          10,
		DefaultMaxOutputTokens: 100,
		PollIntervalSeconds:    60,
		FileRetentionDays:      30,
	}
	seedUser(t, testUser, testToken, testTokenKey)
	seedUser(t, otherUser, otherToken, otherTokenKey)
}

func seedUser(t *testing.T, userId int, tokenId int, key string) {
	t.Helper()
	require.NoError(t, model.DB.Create(&model.User{Id: userId, Username: fmt.Sprintf("user%d", userId), Quota: startQuota,
		Status: common.UserStatusEnabled, Group: "default", AffCode: fmt.Sprintf("aff%d", userId)}).Error)
	require.NoError(t, model.DB.Create(&model.Token{Id: tokenId, UserId: userId, Key: key, Name: "t",
		Status: common.TokenStatusEnabled, RemainQuota: startQuota, ExpiredTime: -1}).Error)
}

func seedBatchChannel(t *testing.T, id int, channelType int, baseURL string) {
	t.Helper()
	channel := &model.Channel{Id: id, Type: channelType, Name: fmt.Sprintf("ch%d", id), Key: fmt.Sprintf("key-%d", id),
		Status: common.ChannelStatusEnabled, Models: testModel, Group: "default", BaseURL: &baseURL}
	require.NoError(t, channel.Insert())
	model.InitChannelCache()
}

func requestContext(userId int, tokenId int, tokenKey string, method string, path string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, path, nil)
	common.SetContextKey(c, constant.ContextKeyUserId, userId)
	common.SetContextKey(c, constant.ContextKeyTokenId, tokenId)
	common.SetContextKey(c, constant.ContextKeyTokenKey, tokenKey)
	common.SetContextKey(c, constant.ContextKeyUsingGroup, "default")
	common.SetContextKey(c, constant.ContextKeyUserGroup, "default")
	c.Set("token_name", "t")
	c.Set("token_quota", startQuota)
	return c, recorder
}

func chatLine(customId string, prompt string, maxTokens int) string {
	return fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":%d}}`,
		customId, testModel, prompt, maxTokens)
}

func upload(t *testing.T, userId int, lines ...string) *model.BatchFile {
	t.Helper()
	file, apiErr := UploadFile(userId, testToken, model.BatchFilePurposeInput, "input.jsonl", []byte(strings.Join(lines, "\n")+"\n"))
	require.Nil(t, apiErr)
	return file
}

func create(t *testing.T, fileId string) (*model.Batch, *Error) {
	t.Helper()
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches")
	return CreateBatch(c, &CreateRequest{InputFileId: fileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
}

func userQuota(t *testing.T, userId int) int {
	t.Helper()
	quota, err := model.GetUserQuota(userId, true)
	require.NoError(t, err)
	return quota
}

func tokenRemain(t *testing.T, tokenId int) int {
	t.Helper()
	token, err := model.GetTokenById(tokenId)
	require.NoError(t, err)
	return token.RemainQuota
}

func poll(t *testing.T, batchId string) *model.Batch {
	t.Helper()
	require.NoError(t, model.DB.Model(&model.Batch{}).Where("batch_id = ?", batchId).Update("next_poll_at", 0).Error)
	PollOnce(t.Context())
	b, err := model.GetBatchByBatchId(batchId)
	require.NoError(t, err)
	return b
}

// expectedQuota is the text price of a batch request: (prompt + completion ×
// completion ratio) × model ratio × group ratio × batch ratio.
func expectedQuota(prompt int, completion int, batchRatio float64) int {
	return int(math.Round((float64(prompt) + float64(completion)*2) * batchRatio))
}

func openRouterTerminal(status string, results string, cost string) string {
	return fmt.Sprintf(`{"id":"orb-1","status":%q,"model":"vendor/model-20260507","request_counts":{"total":2,"completed":2,"failed":0},"usage":{"cost":%s},"results":%s}`, status, cost, results)
}

func orResult(customId string, prompt int, completion int, modelVersion string, cost float64) string {
	return fmt.Sprintf(`{"custom_id":%q,"response":{"status_code":200,"request_id":"req-%s","body":{"id":"gen-%s","model":%q,"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":%d,"completion_tokens":%d,"total_tokens":%d,"cost":%g}}},"error":null}`,
		customId, customId, customId, modelVersion, prompt, completion, prompt+completion, cost)
}

func readJSONL(t *testing.T, userId int, fileId string) []map[string]any {
	t.Helper()
	file, apiErr := GetFileContent(userId, fileId)
	require.Nil(t, apiErr)
	var lines []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(file.Content), []byte("\n")) {
		var line map[string]any
		require.NoError(t, json.Unmarshal(raw, &line))
		lines = append(lines, line)
	}
	return lines
}

// submittedBodies returns the request bodies of the last submission, in order,
// from an OpenRouter or a Gemini create call.
func (f *fakeUpstream) submittedBodies(t *testing.T) [][]byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var submitted struct {
		Requests []struct {
			Body json.RawMessage `json:"body"`
		} `json:"requests"`
		Batch struct {
			InputConfig struct {
				Requests struct {
					Requests []struct {
						Request json.RawMessage `json:"request"`
					} `json:"requests"`
				} `json:"requests"`
			} `json:"inputConfig"`
		} `json:"batch"`
	}
	require.NoError(t, json.Unmarshal(f.lastSubmit, &submitted))
	var bodies [][]byte
	for _, request := range submitted.Requests {
		bodies = append(bodies, request.Body)
	}
	for _, request := range submitted.Batch.InputConfig.Requests.Requests {
		bodies = append(bodies, request.Request)
	}
	return bodies
}

// submittedUsage estimates every request of the last submission as sent,
// with the output limit of its input line.
func submittedUsage(t *testing.T, upstream *fakeUpstream, file *model.BatchFile, defaultMaxOutput int) []*dto.Usage {
	t.Helper()
	lines, apiErr := parseInput(file.Content, "", 100)
	require.Nil(t, apiErr)
	bodies := upstream.submittedBodies(t)
	require.Len(t, bodies, len(lines))
	usages := make([]*dto.Usage, len(lines))
	for index, line := range lines {
		usages[index] = estimateUsage(bodies[index], line.outputLimit(defaultMaxOutput))
	}
	return usages
}

func lastLog(t *testing.T) model.Log {
	t.Helper()
	var log model.Log
	require.NoError(t, model.LOG_DB.Order("id desc").First(&log).Error)
	return log
}

// ---------------------------------------------------------------------------
// Input, request and response formats
// ---------------------------------------------------------------------------

func TestOpenRouterCreateBodyPutsRequestsLast(t *testing.T) {
	body, err := buildOpenRouterCreateBody(&SubmitRequest{
		Endpoint: EndpointChatCompletions, UpstreamModel: "google/gemini", CompletionWindow: CompletionWindow,
		Items: []SubmitItem{{CustomId: "a", Body: []byte(`{"model":"google/gemini"}`)}},
	})
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(body))
	_, err = decoder.Token()
	require.NoError(t, err)
	var keys []string
	for decoder.More() {
		key, err := decoder.Token()
		require.NoError(t, err)
		keys = append(keys, key.(string))
		var skip json.RawMessage
		require.NoError(t, decoder.Decode(&skip))
	}
	require.Equal(t, []string{"endpoint", "model", "completion_window", "requests"}, keys)
}

func TestParseInputRejectsInvalidLines(t *testing.T) {
	valid := chatLine("a", "hi", 5)
	cases := []struct {
		name    string
		content string
		msgID   string
	}{
		{"not json", "{oops", i18n.MsgBatchInvalidLine},
		{"no custom id", `{"method":"POST","url":"/v1/chat/completions","body":{"model":"m"}}`, i18n.MsgBatchInvalidLine},
		{"duplicate custom id", valid + "\n" + valid, i18n.MsgBatchDuplicateCustomId},
		{"wrong method", strings.Replace(valid, `"POST"`, `"GET"`, 1), i18n.MsgBatchInvalidLine},
		{"wrong url", strings.Replace(valid, "/v1/chat/completions", "/v1/embeddings", 1), i18n.MsgBatchInvalidLine},
		{"stream", strings.Replace(valid, `"max_tokens":5`, `"max_tokens":5,"stream":true`, 1), i18n.MsgBatchInvalidLine},
		{"mixed models", valid + "\n" + strings.Replace(chatLine("b", "hi", 5), testModel, "other-model", 1), i18n.MsgBatchMixedModels},
		{"too many", valid + "\n" + chatLine("b", "x", 1) + "\n" + chatLine("c", "x", 1), i18n.MsgBatchTooManyRequests},
		{"empty", "\n\n", i18n.MsgBatchEmptyFile},
		{"empty messages", `{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[]}}`, i18n.MsgBatchInvalidLine},
		{"no messages", `{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"m","prompt":"hi"}}`, i18n.MsgBatchInvalidLine},
		{"negative max tokens", strings.Replace(valid, `"max_tokens":5`, `"max_tokens":-1`, 1), i18n.MsgBatchInvalidLine},
		{"fractional max tokens", strings.Replace(valid, `"max_tokens":5`, `"max_tokens":1.5`, 1), i18n.MsgBatchInvalidLine},
		{"max tokens over the limit", strings.Replace(valid, `"max_tokens":5`, `"max_completion_tokens":1073741824`, 1), i18n.MsgBatchInvalidLine},
		{"wrapped unsigned max tokens", strings.Replace(valid, `"max_tokens":5`, `"max_tokens":18446744073686646784`, 1), i18n.MsgBatchInvalidLine},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, apiErr := parseInput([]byte(tc.content), EndpointChatCompletions, 2)
			require.NotNil(t, apiErr)
			assert.Equal(t, tc.msgID, apiErr.MsgID)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
		})
	}
	lines, apiErr := parseInput([]byte(valid+"\n\n"+chatLine("b", "yo", 7)), EndpointChatCompletions, 2)
	require.Nil(t, apiErr)
	require.Len(t, lines, 2)
	assert.Equal(t, 3, lines[1].Line)
	assert.Equal(t, 7, lines[1].MaxOutputTokens)
}

func TestEstimateUsageCountsBase64MediaByDecodedSize(t *testing.T) {
	audio := strings.Repeat("A", 400_000)
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"caption"},{"type":"input_audio","input_audio":{"data":"` + audio + `","format":"wav"}}]}]}`)
	usage := estimateUsage(body, 50)
	assert.Equal(t, 50, usage.CompletionTokens)
	assert.Equal(t, 300_000/pcmAudioBytesPerToken+1, usage.PromptTokensDetails.AudioTokens)
	assert.Less(t, usage.PromptTokens, 700, "base64 audio must not be priced as text")

	usage = estimateUsage([]byte(strings.Repeat("x", 300)), 8)
	assert.Equal(t, 100, usage.PromptTokens)
	assert.Equal(t, 8, usage.CompletionTokens)
}

func TestParseOpenRouterBatchWaitsForResults(t *testing.T) {
	pending, err := parseOpenRouterBatch([]byte(openRouterTerminal("completed", "null", "0.01")))
	require.NoError(t, err)
	assert.False(t, pending.ResultsReady)

	done, err := parseOpenRouterBatch([]byte(openRouterTerminal("completed", "["+orResult("a", 10, 4, "vendor/model-2026", 0.0003)+"]", "0.0003")))
	require.NoError(t, err)
	require.True(t, done.ResultsReady)
	require.Len(t, done.Results, 1)
	result := done.Results[0]
	assert.Equal(t, "vendor/model-2026", result.Model)
	assert.Equal(t, 10, result.Usage.PromptTokens)
	require.NotNil(t, result.Cost)
	assert.InDelta(t, 0.0003, *result.Cost, 1e-12)
	require.NotNil(t, done.Cost)
}

const geminiSucceeded = `{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_SUCCEEDED","batchStats":{"requestCount":"2","successfulRequestCount":"1","failedRequestCount":"1"}},
"response":{"inlinedResponses":{"inlinedResponses":[
{"response":{"candidates":[{"content":{"parts":[{"text":"alpha"}],"role":"model"},"finishReason":"STOP","index":0}],
 "usageMetadata":{"promptTokenCount":6,"candidatesTokenCount":1,"thoughtsTokenCount":3,"totalTokenCount":10,"promptTokensDetails":[{"modality":"TEXT","tokenCount":6}]},
 "modelVersion":"gemini-3.1-flash-lite-001","responseId":"r1"},"metadata":{"key":"a"}},
{"error":{"code":3,"message":"bad request"},"metadata":{"key":"b"}}]}}}`

func TestParseGeminiOperationKeepsModelVersion(t *testing.T) {
	batch, err := parseGeminiOperation([]byte(geminiSucceeded))
	require.NoError(t, err)
	assert.Equal(t, "completed", batch.Status)
	require.True(t, batch.ResultsReady)
	require.Len(t, batch.Results, 2)

	ok := batch.Results[0]
	assert.True(t, ok.Succeeded())
	assert.Equal(t, "gemini-3.1-flash-lite-001", ok.Model)
	assert.Equal(t, 6, ok.Usage.PromptTokens)
	assert.Equal(t, 4, ok.Usage.CompletionTokens, "thinking tokens are billed as output")
	var body map[string]any
	require.NoError(t, json.Unmarshal(ok.Body, &body))
	assert.Equal(t, "gemini-3.1-flash-lite-001", body["model"])
	assert.Equal(t, "chat.completion", body["object"])

	failed := batch.Results[1]
	assert.False(t, failed.Succeeded())
	assert.Equal(t, http.StatusBadRequest, failed.StatusCode)
	assert.Equal(t, "b", failed.CustomId)

	running, err := parseGeminiOperation([]byte(`{"name":"batches/g1","metadata":{"state":"BATCH_STATE_RUNNING"}}`))
	require.NoError(t, err)
	assert.Equal(t, "in_progress", running.Status)
	assert.False(t, running.ResultsReady)

	cancelled, err := parseGeminiOperation([]byte(`{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_CANCELLED"}}`))
	require.NoError(t, err)
	assert.True(t, cancelled.ResultsReady)
	assert.Empty(t, cancelled.Results)
}

func TestBatchSettingValidation(t *testing.T) {
	valid := map[string]string{
		"batch_setting.enabled": "true", "batch_setting.price_ratio": "0", "batch_setting.max_requests_per_batch": "50000",
		"batch_setting.max_file_size_mb": "200", "batch_setting.poll_interval_seconds": "15", "batch_setting.file_retention_days": "1",
	}
	for key, value := range valid {
		assert.NoError(t, operation_setting.ValidateBatchOption(key, value), key)
	}
	invalid := map[string]string{
		"batch_setting.price_ratio": "-0.5", "batch_setting.max_requests_per_batch": "50001",
		"batch_setting.max_file_size_mb": "0", "batch_setting.poll_interval_seconds": "5", "batch_setting.unknown": "1",
	}
	for key, value := range invalid {
		assert.Error(t, operation_setting.ValidateBatchOption(key, value), key)
	}
	assert.Error(t, operation_setting.ValidateBatchOption("batch_setting.price_ratio", "NaN"))
	assert.Error(t, operation_setting.ValidateBatchOption("batch_setting.price_ratio", "11"))
}

// ---------------------------------------------------------------------------
// Billing
// ---------------------------------------------------------------------------

func TestBatchReservesEstimateAndSettlesFromUpstreamUsage(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", strings.Repeat("x", 30), 20), chatLine("b", strings.Repeat("y", 60), 20))

	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	assert.Equal(t, model.BatchStatusValidating, b.Status)
	assert.Equal(t, "orb-1", b.UpstreamBatchId)
	assert.Equal(t, model.BatchBillingReserved, b.BillingState)
	assert.Equal(t, 1, upstream.submits)
	assert.Contains(t, string(upstream.lastSubmit), `"model":"`+testModel+`"`)

	reserved := 0
	for _, usage := range submittedUsage(t, upstream, file, 100) {
		reserved += expectedQuota(usage.PromptTokens, usage.CompletionTokens, 0.5)
	}
	assert.Equal(t, reserved, b.Quota, "the reservation is the estimate at half price")
	assert.Equal(t, startQuota-reserved, userQuota(t, testUser))
	assert.Equal(t, startQuota-reserved, tokenRemain(t, testToken))

	upstream.set(openRouterTerminal("completed",
		"["+orResult("b", 30, 5, "vendor/model-20260507", 0.0002)+","+orResult("a", 12, 3, "vendor/model-20260507", 0.0001)+"]", "0.0003"))
	settled := poll(t, b.BatchId)
	actual := expectedQuota(12, 3, 0.5) + expectedQuota(30, 5, 0.5)
	assert.Equal(t, model.BatchStatusCompleted, settled.Status)
	assert.Equal(t, model.BatchBillingSettled, settled.BillingState)
	assert.Equal(t, actual, settled.Quota)
	assert.Equal(t, startQuota-actual, userQuota(t, testUser), "the user pays the upstream usage, not the estimate")
	assert.Equal(t, startQuota-actual, tokenRemain(t, testToken))

	output := readJSONL(t, testUser, settled.OutputFileId)
	require.Len(t, output, 2)
	assert.Equal(t, "a", output[0]["custom_id"], "output follows the input order")
	body := output[0]["response"].(map[string]any)["body"].(map[string]any)
	assert.Equal(t, "vendor/model-20260507", body["model"])
	assert.Empty(t, settled.ErrorFileId)

	view := NewBatchObject(settled)
	require.NotNil(t, view.Usage)
	assert.Equal(t, 42, view.Usage.InputTokens)
	assert.Equal(t, map[string]int{"vendor/model-20260507": 2}, view.Usage.ModelVersions)
	assert.Equal(t, "vendor/model-20260507", view.Usage.ResolvedModel)
	summary, _ := common.Marshal(view)
	assert.NotContains(t, string(summary), "upstream_cost", "the upstream cost is not shown to users")

	log := lastLog(t)
	assert.Equal(t, model.LogTypeRefund, log.Type)
	assert.Equal(t, reserved-actual, log.Quota)
	assert.Contains(t, log.Other, `"upstream_cost_usd":0.0003`)

	// A second poller that read the same state must not move funds again.
	b.Status = model.BatchStatusInProgress
	finalize(t.Context(), b, &UpstreamBatch{Status: model.BatchStatusCompleted, ResultsReady: true})
	assert.Equal(t, startQuota-actual, userQuota(t, testUser))
}

func TestBatchChargesMoreWhenUsageExceedsReservation(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 1), chatLine("b", "y", 1))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 5000, 2000, "m", 0)+","+orResult("b", 10, 1, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	actual := expectedQuota(5000, 2000, 0.5) + expectedQuota(10, 1, 0.5)
	require.Greater(t, actual, b.Quota)
	assert.Equal(t, actual, settled.Quota)
	assert.Equal(t, startQuota-actual, userQuota(t, testUser))
	assert.Equal(t, model.LogTypeConsume, lastLog(t).Type)
	assert.Equal(t, actual-b.Quota, lastLog(t).Quota)
}

func TestBatchPartialFailureChargesOnlySucceededRequests(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10), chatLine("b", "y", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	failed := `{"custom_id":"b","response":{"status_code":400,"request_id":"rb","body":{"error":{"message":"bad"}}},"error":{"code":"invalid_request","message":"bad"}}`
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 8, 2, "m", 0)+","+failed+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, expectedQuota(8, 2, 0.5), settled.Quota)
	assert.Equal(t, 1, settled.RequestCompleted)
	assert.Equal(t, 1, settled.RequestFailed)
	errorsOut := readJSONL(t, testUser, settled.ErrorFileId)
	require.Len(t, errorsOut, 1)
	assert.Equal(t, "b", errorsOut[0]["custom_id"])
	assert.Equal(t, "invalid_request", errorsOut[0]["error"].(map[string]any)["code"])
}

func TestBatchSucceededResultWithoutUsageKeepsItsEstimate(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	noUsage := `{"custom_id":"a","response":{"status_code":200,"request_id":"r","body":{"model":"m","choices":[]}},"error":null}`
	upstream.set(openRouterTerminal("completed", "["+noUsage+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, b.Quota, settled.Quota)
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser))
}

func TestBatchExpiredRefundsUnprocessedRequests(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10), chatLine("b", "y", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	upstream.set(openRouterTerminal("expired", "["+orResult("a", 4, 4, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusExpired, settled.Status)
	assert.NotZero(t, settled.ExpiredAt)
	assert.Equal(t, expectedQuota(4, 4, 0.5), settled.Quota)
	assert.Equal(t, startQuota-settled.Quota, userQuota(t, testUser))
	errorsOut := readJSONL(t, testUser, settled.ErrorFileId)
	require.Len(t, errorsOut, 1)
	assert.Equal(t, "batch_expired", errorsOut[0]["error"].(map[string]any)["code"])
}

func TestBatchFailedWithoutResultsRefundsEverything(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	// A batch rejected at validation: one bad line fails the whole batch and
	// results stays null for good.
	upstream.set(`{"id":"orb-1","status":"failed","request_counts":{"total":1,"completed":0,"failed":1},"usage":null,"results":null,"error":{"message":"Invalid request body at line 1"}}`)
	settled := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusFailed, settled.Status)
	assert.Zero(t, settled.Quota)
	assert.Equal(t, startQuota, userQuota(t, testUser))
	assert.Equal(t, startQuota, tokenRemain(t, testToken))
	assert.Contains(t, settled.Errors, "Invalid request body at line 1")
	errorsOut := readJSONL(t, testUser, settled.ErrorFileId)
	require.Len(t, errorsOut, 1)
	assert.Equal(t, "batch_failed", errorsOut[0]["error"].(map[string]any)["code"])
}

func TestParseOpenRouterTerminalWithoutResults(t *testing.T) {
	completed, err := parseOpenRouterBatch([]byte(`{"id":"x","status":"completed","request_counts":{"total":2,"completed":2},"results":null}`))
	require.NoError(t, err)
	assert.False(t, completed.ResultsReady, "a completed batch publishes its results shortly")

	partial, err := parseOpenRouterBatch([]byte(`{"id":"x","status":"expired","request_counts":{"total":2,"completed":1},"results":null}`))
	require.NoError(t, err)
	assert.False(t, partial.ResultsReady, "results of completed requests are still to come")

	rejected, err := parseOpenRouterBatch([]byte(`{"id":"x","status":"failed","request_counts":{"total":2,"completed":0,"failed":1},"results":null}`))
	require.NoError(t, err)
	assert.True(t, rejected.ResultsReady)
	assert.Empty(t, rejected.Results)
}

func TestBatchSettlementUsesTheSubscriptionThatPaid(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	sub := &model.UserSubscription{Id: 7, UserId: testUser, AmountTotal: 1_000_000, AmountUsed: 1000, Status: "active",
		StartTime: time.Now().Unix(), EndTime: time.Now().Add(time.Hour).Unix()}
	require.NoError(t, model.DB.Create(sub).Error)
	b := &model.Batch{BatchId: "batch_sub", UserId: testUser, TokenId: testToken, ChannelId: 1, ChannelType: constant.ChannelTypeOpenRouter,
		Status: model.BatchStatusInProgress, Quota: 500, BillingState: model.BatchBillingReserved,
		BillingSource: "subscription", SubscriptionId: 7, Model: testModel}
	require.NoError(t, model.CreateBatch(b))

	won, err := model.SettleBatch(b.BatchId, map[string]any{"status": model.BatchStatusCompleted, "quota": 200}, nil)
	require.NoError(t, err)
	require.True(t, won)
	settle(t.Context(), b, 200, 0, 0, nil, nil)
	var reloaded model.UserSubscription
	require.NoError(t, model.DB.First(&reloaded, 7).Error)
	assert.Equal(t, int64(1000-300), reloaded.AmountUsed)
	assert.Equal(t, startQuota, userQuota(t, testUser), "the wallet is untouched")
}

func TestBatchRatioSettingOverridesUpstreamDefault(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	operation_setting.GetBatchSetting().PriceRatio = 1
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	var snapshot billingSnapshot
	require.NoError(t, json.Unmarshal([]byte(b.BillingContext), &snapshot))
	assert.Equal(t, 1.0, snapshot.BatchRatio)
	assert.Equal(t, BatchRatioSourceSetting, snapshot.BatchRatioSource)

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 10, 10, "m", 0)+"]", "0"))
	assert.Equal(t, expectedQuota(10, 10, 1), poll(t, b.BatchId).Quota)
}

func TestBatchInsufficientQuotaIsRejectedBeforeSubmission(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", testUser).Update("quota", 10).Error)
	file := upload(t, testUser, chatLine("a", strings.Repeat("x", 3000), 1000))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
	assert.Zero(t, upstream.submits)
	var count int64
	require.NoError(t, model.DB.Model(&model.Batch{}).Count(&count).Error)
	assert.Zero(t, count)
	assert.Equal(t, 10, userQuota(t, testUser))
}

func TestBatchRejectedSubmissionRefundsAndLeavesNoRecord(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	upstream.submitStatus = http.StatusBadRequest
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	assert.Equal(t, i18n.MsgBatchUpstreamError, apiErr.MsgID)
	require.Eventually(t, func() bool { return userQuota(t, testUser) == startQuota }, 2*time.Second, 20*time.Millisecond)
	var count int64
	require.NoError(t, model.DB.Model(&model.Batch{}).Count(&count).Error)
	assert.Zero(t, count)
}

func TestBatchWithoutNativeChannelFails(t *testing.T) {
	resetDB(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenAI, "http://127.0.0.1:1")
	file := upload(t, testUser, chatLine("a", "x", 10))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusServiceUnavailable, apiErr.Status)
	assert.Equal(t, i18n.MsgBatchNoChannel, apiErr.MsgID)
	assert.Equal(t, startQuota, userQuota(t, testUser))
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func TestOpenRouterCancelIsRejected(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches/"+b.BatchId+"/cancel")
	_, apiErr = CancelBatch(c, b.BatchId)
	require.NotNil(t, apiErr)
	assert.Equal(t, "cancel_not_supported", apiErr.Code)
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser), "the reservation stays until the batch ends")
}

func TestGeminiBatchFlowCancelAndModelVersion(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)

	file := upload(t, testUser, chatLine("a", "hello", 10), chatLine("b", "world", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	assert.Equal(t, "batches/g1", b.UpstreamBatchId)
	assert.NotZero(t, b.ExpiresAt)
	var submitted struct {
		Batch struct {
			InputConfig struct {
				Requests struct {
					Requests []struct {
						Request struct {
							Contents         []map[string]any `json:"contents"`
							GenerationConfig map[string]any   `json:"generationConfig"`
						} `json:"request"`
						Metadata map[string]string `json:"metadata"`
					} `json:"requests"`
				} `json:"requests"`
			} `json:"inputConfig"`
		} `json:"batch"`
	}
	require.NoError(t, json.Unmarshal(upstream.lastSubmit, &submitted))
	requests := submitted.Batch.InputConfig.Requests.Requests
	require.Len(t, requests, 2)
	assert.Equal(t, "a", requests[0].Metadata["key"])
	assert.NotEmpty(t, requests[0].Request.Contents, "requests are converted to the Gemini format")
	assert.EqualValues(t, 10, requests[0].Request.GenerationConfig["maxOutputTokens"])

	upstream.set(geminiSucceeded)
	settled := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusCompleted, settled.Status)
	assert.Equal(t, expectedQuota(6, 4, 0.5), settled.Quota)
	output := readJSONL(t, testUser, settled.OutputFileId)
	require.Len(t, output, 1)
	assert.Equal(t, "gemini-3.1-flash-lite-001", output[0]["response"].(map[string]any)["body"].(map[string]any)["model"])

	second := upload(t, testUser, chatLine("c", "again", 10))
	running, apiErr := create(t, second.FileId)
	require.Nil(t, apiErr)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches/x/cancel")
	upstream.set(`{"name":"batches/g1","metadata":{"state":"BATCH_STATE_RUNNING"}}`)
	cancelling, apiErr := CancelBatch(c, running.BatchId)
	require.Nil(t, apiErr)
	assert.Equal(t, model.BatchStatusCancelling, cancelling.Status)
	assert.Equal(t, 1, upstream.cancels)
	assert.Equal(t, model.BatchStatusCancelling, poll(t, running.BatchId).Status, "cancelling stays until the upstream ends")

	before := userQuota(t, testUser)
	upstream.set(`{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_CANCELLED"}}`)
	cancelled := poll(t, running.BatchId)
	assert.Equal(t, model.BatchStatusCancelled, cancelled.Status)
	assert.Zero(t, cancelled.Quota)
	assert.Equal(t, before+running.Quota, userQuota(t, testUser), "a cancelled batch without results is refunded")
}

// geminiAudioResult is a Gemini batch result for an audio caption request:
// audio and text input, thinking tokens and a model version.
func geminiAudioResult(key string, text, audio, candidates, thoughts int) string {
	return fmt.Sprintf(`{"response":{"candidates":[{"content":{"parts":[{"text":"caption"}],"role":"model"},"finishReason":"STOP","index":0}],
 "usageMetadata":{"promptTokenCount":%d,"candidatesTokenCount":%d,"thoughtsTokenCount":%d,"totalTokenCount":%d,
  "promptTokensDetails":[{"modality":"TEXT","tokenCount":%d},{"modality":"AUDIO","tokenCount":%d}]},
 "modelVersion":"gemini-3.1-flash-lite-05-2026"},"metadata":{"key":%q}}`,
		text+audio, candidates, thoughts, text+audio+candidates+thoughts, text, audio, key)
}

func tieredLine(customId string, modelName string, prompt string, maxTokens int) string {
	return fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":%d}}`,
		customId, modelName, prompt, maxTokens)
}

// TestGeminiBatchBillsBuiltinExpressionAtBatchRatio settles a batch of a model
// priced by a built-in expression: audio tokens at the audio price, thinking
// tokens as output, then the batch ratio on top.
func TestGeminiBatchBillsBuiltinExpressionAtBatchRatio(t *testing.T) {
	resetDB(t)
	const modelName = "gemini-3.1-flash-lite"
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	require.NoError(t, model.DB.Model(&model.Channel{}).Where("id = ?", 1).Update("models", modelName).Error)
	model.InitChannelCache()

	file := upload(t, testUser, tieredLine("a", modelName, "caption this", 50), tieredLine("b", modelName, "caption that", 80))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	// $0.25 text, $0.50 audio and $1.50 output per 1M tokens; 1 quota is $0.000002.
	price := func(text, audio, output int, ratio float64) int {
		return int(math.Round((float64(text)*0.25 + float64(audio)*0.5 + float64(output)*1.5) / 1e6 * 500000 * ratio))
	}
	reserved := 0
	for _, usage := range submittedUsage(t, upstream, file, 100) {
		reserved += price(usage.PromptTokens, 0, usage.CompletionTokens, 0.5)
	}
	assert.Equal(t, reserved, b.Quota)
	submitLog := lastLog(t)
	assert.Contains(t, submitLog.Other, `"billing_mode":"tiered_expr"`)
	assert.NotContains(t, submitLog.Other, `"model_ratio"`)

	upstream.set(`{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_SUCCEEDED"},
"response":{"inlinedResponses":{"inlinedResponses":[` + geminiAudioResult("b", 900, 4800, 401, 1100) + `,` + geminiAudioResult("a", 300, 1600, 120, 0) + `]}}}`)
	settled := poll(t, b.BatchId)
	require.Equal(t, model.BatchStatusCompleted, settled.Status)
	want := price(300, 1600, 120, 0.5) + price(900, 4800, 401+1100, 0.5)
	assert.Equal(t, want, settled.Quota)
	assert.Equal(t, startQuota-want, userQuota(t, testUser))
	assert.Equal(t, startQuota-want, tokenRemain(t, testToken))

	view := NewBatchObject(settled)
	require.NotNil(t, view.Usage)
	assert.Equal(t, 6400, view.Usage.InputTokensDetails["audio_tokens"])
	assert.Equal(t, 1100, view.Usage.OutputTokensDetails["reasoning_tokens"])
	assert.Equal(t, map[string]int{"gemini-3.1-flash-lite-05-2026": 2}, view.Usage.ModelVersions)
	settleLog := lastLog(t)
	assert.Contains(t, settleLog.Other, `"billing_mode":"tiered_expr"`)
}

// TestBatchExpressionReadsEachRequestBody prices every request of a batch by
// its own parameters, and keeps the expression frozen when the administrator
// changes it while the batch runs.
func TestBatchExpressionReadsEachRequestBody(t *testing.T) {
	resetDB(t)
	settings := config.GlobalConfig.Get("billing_setting").(*billing_setting.BillingSetting)
	saved := *settings
	t.Cleanup(func() { *settings = saved })
	const modelName = "batch-expr-model"
	settings.BillingMode = map[string]string{modelName: billing_setting.BillingModeTieredExpr}
	settings.BillingExpr = map[string]string{modelName: `param("max_tokens") > 60 ? tier("long", p * 4 + c * 4) : tier("short", p * 2 + c * 2)`}

	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	require.NoError(t, model.DB.Model(&model.Channel{}).Where("id = ?", 1).Update("models", modelName).Error)
	model.InitChannelCache()

	file := upload(t, testUser, tieredLine("a", modelName, "short", 50), tieredLine("b", modelName, "long", 80))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	settings.BillingExpr[modelName] = `tier("changed", p * 100 + c * 100)`
	upstream.set(openRouterTerminal("completed",
		"["+orResult("a", 100, 10, "vendor/model-1", 0)+","+orResult("b", 100, 10, "vendor/model-1", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	// short: 110 tokens * $2, long: 110 tokens * $4, per 1M tokens, at half price.
	assert.Equal(t, int(math.Round(110*2/1e6*500000*0.5))+int(math.Round(110*4/1e6*500000*0.5)), settled.Quota)
}

func TestBatchDeleteNeedsASettledBatch(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodDelete, "/v1/batches/"+b.BatchId)
	apiErr = DeleteBatch(c, b.BatchId)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Status)
	assert.Equal(t, i18n.MsgBatchFileInUse, DeleteFile(testUser, file.FileId).MsgID, "the input of an unsettled batch is kept")

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 3, 3, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	after := userQuota(t, testUser)
	require.Nil(t, DeleteBatch(c, b.BatchId))
	assert.Equal(t, 1, upstream.deletes)
	assert.Equal(t, after, userQuota(t, testUser), "deleting a settled batch moves no funds")
	_, err := model.GetBatch(testUser, b.BatchId)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
	_, apiErr = GetFileContent(testUser, settled.OutputFileId)
	assert.Nil(t, apiErr, "output files outlive the batch")
	assert.Nil(t, DeleteFile(testUser, file.FileId))
}

func TestBatchStateSurvivesRestartAndLostSubmissionIsRefunded(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	// Everything the poller needs is in the database: a fresh poll pass,
	// as after a restart, settles the batch.
	upstream.set(`{"id":"orb-1","status":"in_progress","request_counts":{"total":1,"completed":0,"failed":0}}`)
	assert.Equal(t, model.BatchStatusInProgress, poll(t, b.BatchId).Status)
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 2, 2, "m", 0)+"]", "0"))
	assert.Equal(t, model.BatchStatusCompleted, poll(t, b.BatchId).Status)

	// A crash between saving the record and submitting leaves no upstream id;
	// after the grace period the reservation is refunded.
	lost := &model.Batch{BatchId: "batch_lost", UserId: testUser, TokenId: testToken, ChannelId: 1, ChannelType: constant.ChannelTypeOpenRouter,
		Status: model.BatchStatusValidating, Quota: 400, BillingState: model.BatchBillingReserved, BillingSource: "wallet",
		CreatedAt: time.Now().Add(-time.Hour).Unix(), Model: testModel}
	require.NoError(t, model.CreateBatch(lost))
	before := userQuota(t, testUser)
	refreshed := poll(t, lost.BatchId)
	assert.Equal(t, model.BatchStatusFailed, refreshed.Status)
	assert.Equal(t, before+400, userQuota(t, testUser))
}

func TestBatchUpstreamNotFoundKeepsReservation(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	upstream.mu.Lock()
	upstream.retrieveCode = http.StatusNotFound
	upstream.mu.Unlock()
	for attempt := 1; attempt < notFoundPollsBeforeGiveUp; attempt++ {
		waiting := poll(t, b.BatchId)
		require.Equal(t, model.BatchBillingReserved, waiting.BillingState, "a single not found does not end the batch")
		assert.Equal(t, attempt, waiting.PollFailures)
	}
	failed := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusFailed, failed.Status)
	assert.Equal(t, model.BatchBillingSettled, failed.BillingState)
	assert.Equal(t, b.Quota, failed.Quota)
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser), "the upstream may have billed it")
}

func TestBatchPollBacksOffOnUpstreamErrors(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	upstream.mu.Lock()
	upstream.retrieveCode = http.StatusBadGateway
	upstream.mu.Unlock()
	refreshed := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusValidating, refreshed.Status)
	assert.Equal(t, 1, refreshed.PollFailures)
	assert.Greater(t, refreshed.NextPollAt, common.GetTimestamp()+60)
	assert.Equal(t, model.BatchBillingReserved, refreshed.BillingState)
}

func TestBatchAndFileAccessIsPerUser(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 3, 3, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)

	_, err := model.GetBatch(otherUser, b.BatchId)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
	other, _ := requestContext(otherUser, otherToken, otherTokenKey, http.MethodPost, "/v1/batches")
	_, apiErr = CancelBatch(other, b.BatchId)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Equal(t, http.StatusNotFound, DeleteBatch(other, b.BatchId).Status)
	_, apiErr = GetFileContent(otherUser, settled.OutputFileId)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)
	assert.Equal(t, http.StatusNotFound, DeleteFile(otherUser, file.FileId).Status)
	_, apiErr = CreateBatch(other, &CreateRequest{InputFileId: file.FileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
	assert.Equal(t, http.StatusNotFound, apiErr.Status)

	batches, apiErr := ListBatches(otherUser, "", 20)
	require.Nil(t, apiErr)
	assert.Empty(t, batches.Data)
	files, apiErr := ListFiles(otherUser, "", "", 20)
	require.Nil(t, apiErr)
	assert.Empty(t, files.Data)
	mine, apiErr := ListFiles(testUser, "", "", 20)
	require.Nil(t, apiErr)
	assert.Len(t, mine.Data, 2)

	_, apiErr = GetFileContent(testUser, file.FileId)
	assert.Equal(t, i18n.MsgBatchFileDownloadForbidden, apiErr.MsgID, "input files are not downloadable, as on OpenAI")
}
