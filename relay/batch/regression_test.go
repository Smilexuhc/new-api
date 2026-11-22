package batch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedUpstream answers every request with handler.
type scriptedUpstream struct {
	mu      sync.Mutex
	server  *httptest.Server
	handler http.HandlerFunc
}

func newScriptedUpstream(t *testing.T, handler http.HandlerFunc) *scriptedUpstream {
	s := &scriptedUpstream{handler: handler}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		h := s.handler
		s.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(s.server.Close)
	return s
}

func reloadBatch(t *testing.T, batchId string) *model.Batch {
	t.Helper()
	b, err := model.GetBatchByBatchId(batchId)
	require.NoError(t, err)
	return b
}

func ageBatch(t *testing.T, batchId string, by time.Duration) {
	t.Helper()
	shift := int64(by.Seconds())
	require.NoError(t, model.DB.Exec("UPDATE batches SET created_at = created_at - ?, expires_at = expires_at - ? WHERE batch_id = ?", shift, shift, batchId).Error)
}

func logCount(t *testing.T) int64 {
	t.Helper()
	var n int64
	require.NoError(t, model.LOG_DB.Model(&model.Log{}).Count(&n).Error)
	return n
}

// createLine uploads line and creates a batch from it, returning the error of
// whichever step rejects it.
func createLine(t *testing.T, line string) (*model.Batch, *Error) {
	t.Helper()
	file, apiErr := UploadFile(testUser, testToken, model.BatchFilePurposeInput, "in.jsonl", []byte(line+"\n"))
	if apiErr != nil {
		return nil, apiErr
	}
	return create(t, file.FileId)
}

func setRetrieveCode(f *fakeUpstream, code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retrieveCode = code
}

// ---------------------------------------------------------------------------
// Poller and settlement state
// ---------------------------------------------------------------------------

func TestHugeUpstreamErrorStillSettles(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	upstream.set(`{"id":"orb-1","status":"failed","request_counts":{"total":1,"completed":0,"failed":1},"results":null,"error":{"message":"` + strings.Repeat("E", 70000) + `"}}`)
	settled := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingSettled, settled.BillingState)
	assert.Equal(t, model.BatchStatusFailed, settled.Status)
	assert.Less(t, len(settled.Errors), 2*maxMessageLength, "the message is truncated")
	assert.Equal(t, startQuota, userQuota(t, testUser))
}

func TestRepeatedNotFoundHoldsTheBatch(t *testing.T) {
	resetDB(t)
	huge := strings.Repeat("N", 70000)
	up := newScriptedUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"id":"orb-404","status":"in_progress","request_counts":{"total":1}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"message":"` + huge + `"}}`))
	})
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, up.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	var got *model.Batch
	for i := 0; i < notFoundPollsBeforeHold; i++ {
		got = poll(t, b.BatchId)
	}
	assert.Equal(t, model.BatchBillingHeld, got.BillingState)
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser), "the reservation is kept, not charged or refunded")
	assert.Less(t, len(got.Errors), 2*maxMessageLength)
}

func TestUnknownAndOverlongStatusKeepTheCurrentStatus(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	upstream.set(`{"id":"orb-1","status":"in_progress","request_counts":{"total":1}}`)
	require.Equal(t, model.BatchStatusInProgress, poll(t, b.BatchId).Status)
	upstream.set(`{"id":"orb-1","status":"queued","request_counts":{"total":1}}`)
	assert.Equal(t, model.BatchStatusInProgress, poll(t, b.BatchId).Status)
	upstream.set(`{"id":"orb-1","status":"` + strings.Repeat("s", 40) + `","request_counts":{"total":1}}`)
	got := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusInProgress, got.Status)
	assert.Zero(t, got.PollFailures)
	upstream.set(`{"id":"orb-1","status":"validating","request_counts":{"total":1}}`)
	assert.Equal(t, model.BatchStatusInProgress, poll(t, b.BatchId).Status, "a status does not move backwards")
}

func TestSubmissionWithoutUpstreamIdIsHeld(t *testing.T) {
	resetDB(t)
	up := newScriptedUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"","status":"in_progress","request_counts":{"total":1}}`))
	})
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, up.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr, "the client is told the submission is unconfirmed")

	var held model.Batch
	require.NoError(t, model.DB.Where("user_id = ?", testUser).First(&held).Error)
	assert.Equal(t, model.BatchBillingHeld, held.BillingState)
	assert.Contains(t, held.Errors, "submission_unconfirmed")
	assert.False(t, held.SubmissionUnrecorded)
	assert.Equal(t, startQuota-held.Quota, userQuota(t, testUser))
	assert.Equal(t, held.Quota, usedQuota(t, testUser), "the submission is counted")

	list, listErr := ListHeldBatches(10)
	require.Nil(t, listErr)
	require.Len(t, list, 1)
	assert.Equal(t, held.Quota, list[0].ReservedQuota)

	resolve(t, held.BatchId, ResolveActionRefund, nil)
	assert.Equal(t, startQuota, userQuota(t, testUser))
	assert.Zero(t, usedQuota(t, testUser))
}

func TestNotFoundAfterUnrelatedErrorsDoesNotEndTheBatch(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	for i := 0; i < 3; i++ {
		setRetrieveCode(upstream, http.StatusBadGateway)
		poll(t, b.BatchId)
	}
	setRetrieveCode(upstream, http.StatusNotFound)
	got := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingReserved, got.BillingState)
	assert.Equal(t, 1, got.NotFoundPolls)

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 1, 1, "m", 0)+"]", "0"))
	got = poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusCompleted, got.Status)
	assert.Equal(t, expectedQuota(1, 1, 0.5), got.Quota)
}

func TestTerminalWithoutResultsIsHeldWhenLost(t *testing.T) {
	cases := map[string]struct {
		channelType int
		retrieve    string
	}{
		"openrouter completed": {constant.ChannelTypeOpenRouter, `{"id":"orb-1","status":"completed","request_counts":{"total":1,"completed":1},"results":null}`},
		"gemini succeeded":     {constant.ChannelTypeGemini, `{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_SUCCEEDED"}}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resetDB(t)
			upstream := newFakeUpstream(t)
			seedBatchChannel(t, 1, tc.channelType, upstream.server.URL)
			file := upload(t, testUser, chatLine("a", "x", 10))
			b, apiErr := create(t, file.FileId)
			require.Nil(t, apiErr)
			upstream.set(tc.retrieve)
			assert.Equal(t, model.BatchBillingReserved, poll(t, b.BatchId).BillingState)
			ageBatch(t, b.BatchId, geminiBatchLifetime+upstreamLostAfter+time.Hour)
			held := poll(t, b.BatchId)
			assert.Equal(t, model.BatchBillingHeld, held.BillingState)
			assert.Contains(t, held.Errors, "upstream_unfinished")
			assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser))
		})
	}
}

func TestResultsForUnknownRequestsAreNotCharged(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	parts := []string{orResult("a", 2, 2, "m", 0)}
	for i := 0; i < 20; i++ {
		parts = append(parts, orResult(fmt.Sprintf("ghost-%d", i), 1000, 1000, "m", 0))
	}
	upstream.set(openRouterTerminal("completed", "["+strings.Join(parts, ",")+"]", "0"))
	got := poll(t, b.BatchId)
	assert.Equal(t, expectedQuota(2, 2, 0.5), got.Quota)
	assert.Equal(t, 1, got.RequestCompleted)
}

func TestStalledUpstreamDoesNotBlockOtherBatches(t *testing.T) {
	resetDB(t)
	release := make(chan struct{})
	stall := newScriptedUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"id":"orb-stall","status":"in_progress","request_counts":{"total":1}}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"orb-stall",`))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	healthy := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, stall.server.URL)
	stuck, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	require.NoError(t, model.DB.Exec("DELETE FROM channels").Error)
	require.NoError(t, model.DB.Exec("DELETE FROM abilities").Error)
	seedBatchChannel(t, 2, constant.ChannelTypeOpenRouter, healthy.server.URL)
	ok, apiErr := create(t, upload(t, testUser, chatLine("b", "y", 10)).FileId)
	require.Nil(t, apiErr)
	ch1 := &model.Channel{Id: 1, Type: constant.ChannelTypeOpenRouter, Name: "ch1", Key: "key-1", Status: common.ChannelStatusEnabled,
		Models: testModel, Group: "other", BaseURL: common.GetPointer(stall.server.URL)}
	require.NoError(t, ch1.Insert())
	model.InitChannelCache()
	healthy.set(openRouterTerminal("completed", "["+orResult("b", 1, 1, "m", 0)+"]", "0"))
	require.NoError(t, model.DB.Exec("UPDATE batches SET next_poll_at = -1 WHERE batch_id = ?", stuck.BatchId).Error)
	require.NoError(t, model.DB.Exec("UPDATE batches SET next_poll_at = 0 WHERE batch_id = ?", ok.BatchId).Error)

	done := make(chan struct{})
	go func() { PollOnce(t.Context()); close(done) }()
	assert.Eventually(t, func() bool {
		return reloadBatch(t, ok.BatchId).BillingState == model.BatchBillingSettled
	}, 5*time.Second, 20*time.Millisecond, "the healthy batch settles while the other upstream hangs")
	close(release)
	<-done
}

func TestGeminiCancelSucceedsWhenTheFollowUpRetrieveFails(t *testing.T) {
	resetDB(t)
	var cancels atomic.Int32
	up := newScriptedUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":cancel"):
			cancels.Add(1)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodPost, cancels.Load() == 0:
			_, _ = w.Write([]byte(`{"name":"batches/gx","metadata":{"state":"BATCH_STATE_RUNNING"}}`))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"message":"busy"}}`))
		}
	})
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, up.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches/x/cancel")
	cancelled, cerr := CancelBatch(c, b.BatchId)
	require.Nil(t, cerr)
	assert.EqualValues(t, 1, cancels.Load())
	assert.Equal(t, model.BatchStatusCancelling, cancelled.Status)
}

func TestCancelledBatchTheUpstreamCompletedEndsCancelled(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10), chatLine("b", "y", 10)).FileId)
	require.Nil(t, apiErr)
	upstream.set(`{"name":"batches/g1","metadata":{"state":"BATCH_STATE_PENDING"}}`)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches/x/cancel")
	_, e1 := CancelBatch(c, b.BatchId)
	_, e2 := CancelBatch(c, b.BatchId)
	require.Nil(t, e1)
	require.Nil(t, e2)
	assert.Equal(t, 1, upstream.cancels, "a repeated cancel does not reach the upstream again")

	upstream.set(geminiSucceeded)
	got := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusCancelled, got.Status)
	assert.Equal(t, model.BatchBillingSettled, got.BillingState)
	assert.Positive(t, got.Quota, "what the upstream ran is charged")
	assert.Equal(t, startQuota-got.Quota, userQuota(t, testUser))
}

func TestCancelOfABatchTheUpstreamFinishedIsRefused(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	upstream.set(geminiSucceeded)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches/x/cancel")
	_, cerr := CancelBatch(c, b.BatchId)
	require.NotNil(t, cerr)
	assert.Equal(t, http.StatusConflict, cerr.Status)
	assert.Zero(t, upstream.cancels, "a finished batch is not cancelled upstream")

	got := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusCompleted, got.Status)
	assert.Equal(t, model.BatchBillingSettled, got.BillingState)
}

func TestGeminiCancelledWithSucceededRequestsButNoResultsIsHeld(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10), chatLine("b", "y", 10)).FileId)
	require.Nil(t, apiErr)
	upstream.set(`{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_CANCELLED","batchStats":{"requestCount":"2","successfulRequestCount":"2"}}}`)
	held := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingHeld, held.BillingState)
	assert.Contains(t, held.Errors, "upstream_results_lost")
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser), "the reservation is kept, not refunded")

	upstream.set(`{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_CANCELLED","batchStats":{"requestCount":"2"}}}`)
	refunded := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingHeld, refunded.BillingState, "a held batch is not polled")
}

func TestLostChannelKeyHoldsAndResumeSettles(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	var original model.Channel
	require.NoError(t, model.DB.First(&original, 1).Error)
	require.NoError(t, model.DB.Model(&model.Channel{}).Where("id = ?", 1).Update("key", "rotated-key").Error)
	model.InitChannelCache()
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 1, 1, "m", 0)+"]", "0"))

	var got *model.Batch
	for i := 0; i < unavailablePollsBeforeHold; i++ {
		got = poll(t, b.BatchId)
	}
	assert.Equal(t, model.BatchBillingHeld, got.BillingState)
	assert.Contains(t, got.Errors, "channel_unavailable")
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser))

	require.NoError(t, model.DB.Model(&model.Channel{}).Where("id = ?", 1).Update("key", original.Key).Error)
	model.InitChannelCache()
	resolve(t, b.BatchId, ResolveActionResume, nil)
	settled := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingSettled, settled.BillingState)
	assert.Equal(t, expectedQuota(1, 1, 0.5), settled.Quota)
	assert.Equal(t, startQuota-settled.Quota, userQuota(t, testUser))
}

func TestUnauthorizedChannelHoldsAfterRepeatedFailures(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	setRetrieveCode(upstream, http.StatusUnauthorized)
	for i := 1; i < unavailablePollsBeforeHold; i++ {
		require.Equal(t, model.BatchBillingReserved, poll(t, b.BatchId).BillingState)
	}
	held := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingHeld, held.BillingState)
	assert.Contains(t, held.Errors, "channel_unauthorized")
}

func TestConcurrentFinalizeSettlesOnce(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", strings.Repeat("x", 300), 50), chatLine("b", "y", 50)).FileId)
	require.Nil(t, apiErr)
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 10, 2, "m", 0)+","+orResult("b", 10, 2, "m", 0)+"]", "0"))
	ub, err := GetAdaptor(constant.ChannelTypeOpenRouter).Retrieve(t.Context(), &Connection{ChannelType: constant.ChannelTypeOpenRouter, BaseURL: upstream.server.URL, Key: "k"}, "orb-1")
	require.NoError(t, err)
	logsBefore := logCount(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		copyB := *b
		wg.Add(1)
		go func() {
			defer wg.Done()
			finalize(t.Context(), &copyB, ub)
		}()
	}
	wg.Wait()
	final := reloadBatch(t, b.BatchId)
	assert.Equal(t, model.BatchBillingSettled, final.BillingState)
	assert.Equal(t, startQuota-final.Quota, userQuota(t, testUser))
	assert.Equal(t, startQuota-final.Quota, tokenRemain(t, testToken))
	assert.Equal(t, int64(1), logCount(t)-logsBefore)
	var outputs int64
	require.NoError(t, model.DB.Model(&model.BatchFile{}).Where("batch_id = ?", b.BatchId).Count(&outputs).Error)
	assert.Equal(t, int64(1), outputs)
}

func TestLargeOutputIsStoredInParts(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	lines := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		lines = append(lines, chatLine(fmt.Sprintf("r%d", i), "x", 10))
	}
	b, apiErr := create(t, upload(t, testUser, lines...).FileId)
	require.Nil(t, apiErr)
	content := strings.Repeat("y", 900_000)
	results := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		results = append(results, fmt.Sprintf(`{"custom_id":"r%d","response":{"status_code":200,"body":{"model":"m","choices":[{"message":{"role":"assistant","content":%q}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}},"error":null}`, i, content))
	}
	upstream.set(openRouterTerminal("completed", "["+strings.Join(results, ",")+"]", "0"))
	settled := poll(t, b.BatchId)
	require.Equal(t, model.BatchBillingSettled, settled.BillingState)
	var parts int64
	require.NoError(t, model.DB.Model(&model.BatchFilePart{}).Count(&parts).Error)
	assert.GreaterOrEqual(t, parts, int64(5))
	output := readJSONL(t, testUser, settled.OutputFileId)
	require.Len(t, output, 6)

	require.Nil(t, DeleteFile(testUser, settled.OutputFileId))
	require.NoError(t, model.DB.Model(&model.BatchFilePart{}).Count(&parts).Error)
	assert.Zero(t, parts, "deleting a file deletes its parts")
}

// ---------------------------------------------------------------------------
// Billing
// ---------------------------------------------------------------------------

func TestGeminiGroundingIsBilledPerQuery(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	line := fmt.Sprintf(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"news today"}],"max_tokens":10,"tools":[{"type":"function","function":{"name":"googleSearch","parameters":{"type":"object","properties":{}}}}]}}`, testModel)
	b, apiErr := create(t, upload(t, testUser, line).FileId)
	require.Nil(t, apiErr)
	require.Contains(t, string(upstream.lastSubmit), `"googleSearch"`)

	upstream.set(`{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_SUCCEEDED"},
"response":{"inlinedResponses":{"inlinedResponses":[
{"response":{"candidates":[{"content":{"parts":[{"text":"grounded"}],"role":"model"},"finishReason":"STOP","index":0,
 "groundingMetadata":{"webSearchQueries":["q1","q2","q3","q4","q5","q5"]}}],
 "usageMetadata":{"promptTokenCount":6,"candidatesTokenCount":4,"totalTokenCount":10},
 "modelVersion":"gemini-3-flash","responseId":"r1"},"metadata":{"key":"a"}}]}}}`)
	settled := poll(t, b.BatchId)
	require.Equal(t, model.BatchBillingSettled, settled.BillingState)

	var snapshot billingSnapshot
	require.NoError(t, json.Unmarshal([]byte(b.BillingContext), &snapshot))
	usage := &dto.Usage{PromptTokens: 6, CompletionTokens: 4, TotalTokens: 10}
	tokenOnly, err := service.PriceTextUsage(&gin.Context{}, snapshot.relayInfo(), usage)
	require.NoError(t, err)
	withTool := snapshot.relayInfo()
	withTool.SetBillableToolCount(dto.BuildInToolGoogleSearch, 5)
	withTool.SetBillableToolCount(relaycommon.GoogleSearchGroundedPromptTool, 1)
	grounded, err := service.PriceTextUsage(&gin.Context{}, withTool, usage)
	require.NoError(t, err)
	require.Greater(t, grounded.Quota, tokenOnly.Quota)
	assert.Equal(t, grounded.Quota, settled.Quota, "five unique queries are billed like the synchronous relay")
	assert.GreaterOrEqual(t, b.Quota, settled.Quota, "the reservation covers the queries")
}

func TestSubscriptionOverrun(t *testing.T) {
	for _, allowOverflow := range []bool{true, false} {
		t.Run(fmt.Sprintf("wallet overflow allowed %v", allowOverflow), func(t *testing.T) {
			resetDB(t)
			upstream := newFakeUpstream(t)
			seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
			b, apiErr := create(t, upload(t, testUser, mediaLine("a", testModel, `{"type":"video_url","video_url":{"url":"https://cdn.example.com/1h.mp4"}}`, 200)).FileId)
			require.Nil(t, apiErr)
			reserved := b.Quota

			// Move the reservation onto a nearly full subscription.
			require.NoError(t, model.IncreaseUserQuota(testUser, reserved, true))
			sub := &model.UserSubscription{Id: 9, UserId: testUser, AmountTotal: int64(reserved) + 1000, AmountUsed: int64(reserved) + 900, Status: "active",
				StartTime: time.Now().Unix(), EndTime: time.Now().Add(time.Hour).Unix(), AllowWalletOverflow: allowOverflow}
			require.NoError(t, model.DB.Create(sub).Error)
			require.NoError(t, model.DB.Model(&model.Batch{}).Where("batch_id = ?", b.BatchId).
				Updates(map[string]any{"billing_source": "subscription", "subscription_id": 9}).Error)
			tokenBefore := tokenRemain(t, testToken)
			logsBefore := logCount(t)

			upstream.set(openRouterTerminal("completed", "["+orResult("a", 1_080_000, 150, "vendor/video-1", 0)+"]", "0"))
			settled := poll(t, b.BatchId)
			actual := expectedQuota(1_080_000, 150, 0.5)
			require.Equal(t, actual, settled.Quota)
			overrun := actual - reserved

			var reloaded model.UserSubscription
			require.NoError(t, model.DB.First(&reloaded, 9).Error)
			assert.Equal(t, reloaded.AmountTotal, reloaded.AmountUsed, "the subscription is charged up to its total")
			assert.Equal(t, int64(1), logCount(t)-logsBefore)
			if allowOverflow {
				assert.Equal(t, startQuota-(overrun-100), userQuota(t, testUser), "the rest is charged to the wallet")
				assert.Equal(t, tokenBefore-overrun, tokenRemain(t, testToken))
				assert.Contains(t, lastLog(t).Other, "wallet_overflow_quota")
				return
			}
			assert.Equal(t, startQuota, userQuota(t, testUser), "the wallet is not charged")
			assert.Equal(t, tokenBefore-100, tokenRemain(t, testToken), "the token is charged what was collected")
			assert.Equal(t, 100, lastLog(t).Quota)
			assert.Contains(t, lastLog(t).Other, "uncollected_quota")
		})
	}
}

func TestWalletOverrunIsChargedInFull(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, mediaLine("a", testModel, `{"type":"video_url","video_url":{"url":"https://cdn.example.com/1h.mp4"}}`, 200)).FileId)
	require.Nil(t, apiErr)
	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", testUser).Update("quota", 0).Error)
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 1_080_000, 150, "vendor/video-1", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, -(settled.Quota - b.Quota), userQuota(t, testUser), "the upstream billed it, so the wallet owes the rest")
}

func TestUnusableUsageIsChargedTheEstimate(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10), chatLine("b", "y", 10)).FileId)
	require.Nil(t, apiErr)
	upstream.set(openRouterTerminal("completed", "["+orResult("a", -1_000_000, -5, "m", 0)+","+orResult("b", 0, 0, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, b.Quota, settled.Quota)
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser))
}

func chatLineN(customId string, prompt string, maxTokens int, n int) string {
	return fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":%d,"n":%d}}`,
		customId, testModel, prompt, maxTokens, n)
}

func TestChoicesMultiplyTheReservation(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	single, apiErr := create(t, upload(t, testUser, chatLineN("a", "x", 100, 1)).FileId)
	require.Nil(t, apiErr)
	eight, apiErr := create(t, upload(t, testUser, chatLineN("a", "x", 100, 8)).FileId)
	require.Nil(t, apiErr)
	assert.GreaterOrEqual(t, eight.Quota, expectedQuota(0, 800, 0.5))
	assert.Greater(t, eight.Quota, 7*single.Quota)

	_, apiErr = createLine(t, chatLineN("a", "x", 100, 9))
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)

	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", testUser).Update("quota", eight.Quota-1).Error)
	_, apiErr = createLine(t, chatLineN("a", "x", 100, 8))
	require.NotNil(t, apiErr, "a user who cannot pay n completions is rejected")
}

func TestUpstreamCostAboveTheChargeIsFlagged(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "hi", 10)).FileId)
	require.Nil(t, apiErr)
	upstream.set(openRouterTerminal("completed", "["+orResult("a", 10, 10, "m", 5)+"]", "5"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, expectedQuota(10, 10, 0.5), settled.Quota)
	assert.Contains(t, lastLog(t).Other, "upstream_cost_exceeds_charge")
}

// ---------------------------------------------------------------------------
// Request fields and media
// ---------------------------------------------------------------------------

func TestBatchRejectsFieldsThatChangeWhatTheUpstreamRuns(t *testing.T) {
	cases := map[string]struct {
		channelType int
		extra       string
	}{
		"models":                    {constant.ChannelTypeOpenRouter, `"models":["anthropic/claude-opus-4"]`},
		"plugins":                   {constant.ChannelTypeOpenRouter, `"plugins":[{"id":"web"}]`},
		"provider":                  {constant.ChannelTypeOpenRouter, `"provider":{"order":["Anthropic"]}`},
		"hosted tool":               {constant.ChannelTypeOpenRouter, `"tools":[{"type":"web_search"}]`},
		"web search on openrouter":  {constant.ChannelTypeOpenRouter, `"web_search_options":{}`},
		"hosted tool on gemini too": {constant.ChannelTypeGemini, `"tools":[{"type":"code_interpreter"}]`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resetDB(t)
			upstream := newFakeUpstream(t)
			seedBatchChannel(t, 1, tc.channelType, upstream.server.URL)
			line := fmt.Sprintf(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":10,%s}}`, testModel, tc.extra)
			_, apiErr := createLine(t, line)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Zero(t, upstream.submits)
			assert.Equal(t, startQuota, userQuota(t, testUser))
		})
	}
}

func TestBatchDropsFieldsTheSynchronousRelayDrops(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	line := fmt.Sprintf(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":10,"service_tier":"priority","safety_identifier":"victim@example.com","store":true}}`, testModel)
	_, apiErr := create(t, upload(t, testUser, line).FileId)
	require.Nil(t, apiErr)
	bodies := upstream.submittedBodies(t)
	require.Len(t, bodies, 1)
	var sent map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(bodies[0], &sent))
	for _, field := range []string{"service_tier", "safety_identifier", "store"} {
		assert.NotContains(t, sent, field)
	}
}

type countingMediaServer struct {
	server *httptest.Server
	hits   atomic.Int32
	bytes  atomic.Int64
}

func newCountingMediaServer(t *testing.T, payload []byte) *countingMediaServer {
	s := &countingMediaServer{}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		w.Header().Set("Content-Type", "video/mp4")
		n, _ := w.Write(payload)
		s.bytes.Add(int64(n))
	}))
	t.Cleanup(s.server.Close)
	return s
}

func createGeminiMediaBatch(t *testing.T, lines ...string) *Error {
	t.Helper()
	file := upload(t, testUser, lines...)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches")
	t.Cleanup(func() { service.CleanupFileSources(c) })
	_, apiErr := CreateBatch(c, &CreateRequest{InputFileId: file.FileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
	return apiErr
}

func TestUserWhoCannotPayDownloadsNothing(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	operation_setting.GetBatchSetting().MaxRequestsPerBatch = 1000
	media := newCountingMediaServer(t, fakeVideo(2048))
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", testUser).Update("quota", 0).Error)
	lines := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		lines = append(lines, mediaLine(fmt.Sprintf("r%d", i), testModel, fmt.Sprintf(`{"type":"video_url","video_url":{"url":"%s/v.mp4?n=%d"}}`, media.server.URL, i), 10))
	}
	apiErr := createGeminiMediaBatch(t, lines...)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusForbidden, apiErr.Status)
	assert.Zero(t, media.hits.Load())
	assert.Zero(t, upstream.submits)
}

func TestDownloadsStopAtTheInlineLimit(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	media := newCountingMediaServer(t, fakeVideo(4<<20))
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	parts := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		parts = append(parts, fmt.Sprintf(`{"type":"video_url","video_url":{"url":"%s/v.mp4?n=%d"}}`, media.server.URL, i))
	}
	apiErr := createGeminiMediaBatch(t, mediaLine("a", testModel, strings.Join(parts, ","), 10))
	require.NotNil(t, apiErr)
	assert.Equal(t, i18n.MsgBatchInlineTooLarge, apiErr.MsgID)
	assert.LessOrEqual(t, media.hits.Load(), int32(6))
	assert.Zero(t, upstream.submits)
	assert.Eventually(t, func() bool { return userQuota(t, testUser) == startQuota }, 2*time.Second, 10*time.Millisecond)
}

func TestTooManyLinkedMediaIsRejectedBeforeDownloading(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	media := newCountingMediaServer(t, fakeVideo(2048))
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	parts := make([]string, 0, maxRemoteMediaPerBatch+1)
	for i := 0; i <= maxRemoteMediaPerBatch; i++ {
		parts = append(parts, fmt.Sprintf(`{"type":"image_url","image_url":{"url":"%s/i.png?n=%d"}}`, media.server.URL, i))
	}
	apiErr := createGeminiMediaBatch(t, mediaLine("a", testModel, strings.Join(parts, ","), 10))
	require.NotNil(t, apiErr)
	assert.Equal(t, i18n.MsgBatchTooManyRemoteMedia, apiErr.MsgID)
	assert.Zero(t, media.hits.Load())
}

func TestLinkedVideoIsReservedByItsSizeNotTheAllowance(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	media := newMediaServer(t, fakeVideo(300_000))
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	// A user who could not pay the ten-minute allowance can still caption a
	// short clip.
	allowance := expectedQuota(remoteVideoTokens, 0, 0.5)
	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", testUser).Update("quota", allowance/2).Error)
	part := `{"type":"video_url","video_url":{"url":"` + media.url() + `"}}`
	file := upload(t, testUser, mediaLine("a", testModel, part, 50))
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches")
	t.Cleanup(func() { service.CleanupFileSources(c) })
	b, apiErr := CreateBatch(c, &CreateRequest{InputFileId: file.FileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
	require.Nil(t, apiErr)
	assert.Less(t, b.Quota, allowance/10)
	assert.GreaterOrEqual(t, b.Quota, expectedQuota(300_000/videoBytesPerToken, 50, 0.5))
}

// ---------------------------------------------------------------------------
// Information exposure and storage
// ---------------------------------------------------------------------------

func TestUpstreamErrorHidesTheChannelAddress(t *testing.T) {
	resetDB(t)
	var baseURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `{"error":{"message":"request to %s/v1/batches failed via proxy 10.20.30.40:3128"}}`, baseURL)
	}))
	t.Cleanup(server.Close)
	baseURL = server.URL
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, baseURL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches")
	_, apiErr := CreateBatch(c, &CreateRequest{InputFileId: file.FileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
	require.NotNil(t, apiErr)
	text := apiErr.Text(c)
	assert.NotContains(t, text, strings.TrimPrefix(baseURL, "http://"))
	assert.NotContains(t, text, "10.20.30.40")
}

func TestInputFilesPerUserAreLimited(t *testing.T) {
	resetDB(t)
	operation_setting.GetBatchSetting().MaxInputFilesPerUser = 3
	line := chatLine("a", "x", 10) + "\n"
	for i := 0; i < 3; i++ {
		_, apiErr := UploadFile(testUser, testToken, model.BatchFilePurposeInput, "in.jsonl", []byte(line))
		require.Nil(t, apiErr)
	}
	_, apiErr := UploadFile(testUser, testToken, model.BatchFilePurposeInput, "in.jsonl", []byte(line))
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusTooManyRequests, apiErr.Status)
	_, apiErr = UploadFile(otherUser, otherToken, model.BatchFilePurposeInput, "in.jsonl", []byte(line))
	assert.Nil(t, apiErr, "the limit is per user")
}

func TestCancelIsRefusedWhenTheUpstreamStateCannotBeRead(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	setRetrieveCode(upstream, http.StatusServiceUnavailable)
	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches/x/cancel")
	_, cerr := CancelBatch(c, b.BatchId)
	require.NotNil(t, cerr)
	assert.Zero(t, upstream.cancels, "a batch that may have finished is not cancelled")
	assert.Equal(t, model.BatchStatusValidating, reloadBatch(t, b.BatchId).Status)
}

func TestSubmissionFailures(t *testing.T) {
	cases := map[string]struct {
		handler  http.HandlerFunc
		closed   bool
		unknown  bool
		heldCode string
	}{
		"rejected by the upstream": {handler: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"bad request"}}`))
		}},
		"upstream unreachable": {closed: true},
		"answer lost": {unknown: true, handler: func(w http.ResponseWriter, r *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}},
		"gateway timeout": {unknown: true, handler: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusGatewayTimeout)
		}},
		"unreadable acceptance": {unknown: true, handler: func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id":`))
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resetDB(t)
			server := httptest.NewServer(tc.handler)
			if tc.closed {
				server.Close()
			} else {
				t.Cleanup(server.Close)
			}
			seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, server.URL)
			_, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
			require.NotNil(t, apiErr)

			var batches []model.Batch
			require.NoError(t, model.DB.Where("user_id = ?", testUser).Find(&batches).Error)
			if !tc.unknown {
				assert.Empty(t, batches, "a submission that never ran leaves no batch")
				assert.Equal(t, startQuota, userQuota(t, testUser))
				return
			}
			require.Len(t, batches, 1)
			assert.Equal(t, model.BatchBillingHeld, batches[0].BillingState)
			assert.Contains(t, batches[0].Errors, "submission_unconfirmed")
			assert.Equal(t, startQuota-batches[0].Quota, userQuota(t, testUser), "the reservation waits for an administrator")
		})
	}
}

func TestBatchFollowsItsKeyToAnotherChannel(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	b, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	require.NoError(t, model.DB.Delete(&model.Channel{}, 1).Error)
	moved := &model.Channel{Id: 2, Type: constant.ChannelTypeOpenRouter, Name: "ch2", Key: "other-key\nkey-1",
		Status: common.ChannelStatusEnabled, Models: testModel, Group: "default", BaseURL: common.GetPointer(upstream.server.URL)}
	require.NoError(t, moved.Insert())
	model.InitChannelCache()

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 1, 1, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingSettled, settled.BillingState)
	assert.Equal(t, expectedQuota(1, 1, 0.5), settled.Quota)
}

func TestStalledReadDoesNotHoldUpLaterRounds(t *testing.T) {
	resetDB(t)
	previous := pollRoundBudget
	pollRoundBudget = 200 * time.Millisecond
	t.Cleanup(func() { pollRoundBudget = previous })
	release := make(chan struct{})
	var stalledReads atomic.Int32
	stall := newScriptedUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"id":"orb-stall","status":"in_progress","request_counts":{"total":1}}`))
			return
		}
		stalledReads.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, stall.server.URL)
	stuck, apiErr := create(t, upload(t, testUser, chatLine("a", "x", 10)).FileId)
	require.Nil(t, apiErr)
	require.NoError(t, model.DB.Model(&model.Batch{}).Where("batch_id = ?", stuck.BatchId).Update("next_poll_at", 0).Error)

	started := time.Now()
	PollOnce(t.Context())
	assert.Less(t, time.Since(started), 2*time.Second, "the round does not wait for the stalled read")
	assert.Greater(t, reloadBatch(t, stuck.BatchId).NextPollAt, common.GetTimestamp(), "the stalled batch is claimed")

	healthy := newFakeUpstream(t)
	seedBatchChannel(t, 2, constant.ChannelTypeOpenRouter, healthy.server.URL)
	require.NoError(t, model.DB.Model(&model.Channel{}).Where("id = ?", 1).Update("status", common.ChannelStatusManuallyDisabled).Error)
	require.NoError(t, model.DB.Model(&model.Ability{}).Where("channel_id = ?", 1).Update("enabled", false).Error)
	model.InitChannelCache()
	ok, apiErr := create(t, upload(t, testUser, chatLine("b", "y", 10)).FileId)
	require.Nil(t, apiErr)
	healthy.set(openRouterTerminal("completed", "["+orResult("b", 1, 1, "m", 0)+"]", "0"))
	assert.Equal(t, model.BatchBillingSettled, poll(t, ok.BatchId).BillingState)
	assert.EqualValues(t, 1, stalledReads.Load(), "the claimed batch is not read again")
}
