package batch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// Media fixtures
// ---------------------------------------------------------------------------

// fakeVideo returns size bytes that start like an MP4 file.
func fakeVideo(size int) []byte {
	video := make([]byte, size)
	copy(video, "\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom")
	for index := 24; index < size; index++ {
		video[index] = byte(index * 31)
	}
	return video
}

type mediaServer struct {
	server *httptest.Server
	hits   atomic.Int32
}

// newMediaServer serves video at /clip.mp4 and counts the downloads.
func newMediaServer(t *testing.T, video []byte) *mediaServer {
	m := &mediaServer{}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.hits.Add(1)
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(video)
	}))
	t.Cleanup(m.server.Close)
	return m
}

func (m *mediaServer) url() string { return m.server.URL + "/clip.mp4" }

// allowMediaDownloads lets the Gemini converter fetch from the local test
// server, which SSRF protection blocks by default.
func allowMediaDownloads(t *testing.T) {
	fetch := system_setting.GetFetchSetting()
	saved := *fetch
	savedMax := constant.MaxFileDownloadMB
	fetch.EnableSSRFProtection = false
	constant.MaxFileDownloadMB = 64
	if service.GetHttpClient() == nil {
		service.InitHttpClient()
	}
	t.Cleanup(func() {
		*fetch = saved
		constant.MaxFileDownloadMB = savedMax
	})
}

// mediaLine is a chat request with a text part and one media part.
func mediaLine(customId string, modelName string, mediaPart string, maxTokens int) string {
	limit := ""
	if maxTokens > 0 {
		limit = fmt.Sprintf(`,"max_tokens":%d`, maxTokens)
	}
	return fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":[{"type":"text","text":"describe the clip"},%s]}]%s}}`,
		customId, modelName, mediaPart, limit)
}

// geminiMediaResult is a Gemini batch result whose prompt has text, video and
// audio tokens.
func geminiMediaResult(key string, text, video, audio, candidates int) string {
	details := fmt.Sprintf(`{"modality":"TEXT","tokenCount":%d},{"modality":"VIDEO","tokenCount":%d}`, text, video)
	if audio > 0 {
		details += fmt.Sprintf(`,{"modality":"AUDIO","tokenCount":%d}`, audio)
	}
	prompt := text + video + audio
	return fmt.Sprintf(`{"response":{"candidates":[{"content":{"parts":[{"text":"a cat"}],"role":"model"},"finishReason":"STOP","index":0}],
 "usageMetadata":{"promptTokenCount":%d,"candidatesTokenCount":%d,"totalTokenCount":%d,"promptTokensDetails":[%s]},
 "modelVersion":"gemini-video-001","responseId":"r-%s"},"metadata":{"key":%q}}`, prompt, candidates, prompt+candidates, details, key, key)
}

func geminiSucceededWith(results ...string) string {
	return `{"name":"batches/g1","done":true,"metadata":{"state":"BATCH_STATE_SUCCEEDED"},"response":{"inlinedResponses":{"inlinedResponses":[` +
		strings.Join(results, ",") + `]}}}`
}

// submittedGeminiParts returns the parts of the first content of every
// request in the last Gemini submission.
func submittedGeminiParts(t *testing.T, upstream *fakeUpstream) [][]map[string]any {
	t.Helper()
	var parts [][]map[string]any
	for _, body := range upstream.submittedBodies(t) {
		var request struct {
			Contents []struct {
				Parts []map[string]any `json:"parts"`
			} `json:"contents"`
		}
		require.NoError(t, json.Unmarshal(body, &request))
		require.NotEmpty(t, request.Contents)
		parts = append(parts, request.Contents[0].Parts)
	}
	return parts
}

func submittedGenerationConfig(t *testing.T, upstream *fakeUpstream, index int) map[string]any {
	t.Helper()
	var request struct {
		GenerationConfig map[string]any `json:"generationConfig"`
	}
	require.NoError(t, json.Unmarshal(upstream.submittedBodies(t)[index], &request))
	return request.GenerationConfig
}

func reservedFor(t *testing.T, upstream *fakeUpstream, file *model.BatchFile) int {
	t.Helper()
	reserved := 0
	for _, usage := range submittedUsage(t, upstream, file, 100) {
		reserved += expectedQuota(usage.PromptTokens, usage.CompletionTokens, 0.5)
	}
	return reserved
}

func countBatches(t *testing.T) int64 {
	t.Helper()
	var count int64
	require.NoError(t, model.DB.Model(&model.Batch{}).Count(&count).Error)
	return count
}

func usedQuota(t *testing.T, userId int) int {
	t.Helper()
	var user model.User
	require.NoError(t, model.DB.Select("used_quota").First(&user, userId).Error)
	return user.UsedQuota
}

// failBatchWrites makes updates or deletes of the batches table fail while
// the returned flag is set.
func failBatchWrites(t *testing.T, deletes bool) *atomic.Bool {
	failing := &atomic.Bool{}
	failing.Store(true)
	name := "review:fail_batch_update"
	processor := model.DB.Callback().Update()
	before := "gorm:update"
	if deletes {
		name = "review:fail_batch_delete"
		processor = model.DB.Callback().Delete()
		before = "gorm:delete"
	}
	require.NoError(t, processor.Before(before).Register(name, func(db *gorm.DB) {
		if failing.Load() && db.Statement.Schema != nil && db.Statement.Schema.Table == "batches" {
			_ = db.AddError(errors.New("database is unavailable"))
		}
	}))
	t.Cleanup(func() { _ = processor.Remove(name) })
	return failing
}

// ---------------------------------------------------------------------------
// Estimates
// ---------------------------------------------------------------------------

func TestEstimateUsageCountsEveryMediaKind(t *testing.T) {
	b64 := func(decoded int) string { return strings.Repeat("A", decoded/3*4) }
	chat := func(part string) string {
		return `{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"describe"},` + part + `]}]}`
	}
	gemini := func(part string) string {
		return `{"contents":[{"role":"user","parts":[{"text":"describe"},` + part + `]}]}`
	}
	cases := []struct {
		name   string
		body   string
		visual int
		audio  int
	}{
		{"two linked images", chat(`{"type":"image_url","image_url":{"url":"https://example.com/a.png"}},{"type":"image_url","image_url":"https://example.com/b.png"}`), 2 * remoteImageTokens, 0},
		{"linked video as a string", chat(`{"type":"video_url","video_url":"https://example.com/a.mp4"}`), remoteVideoTokens, 0},
		{"linked video as an object", chat(`{"type":"video_url","video_url":{"url":"https://example.com/a.mp4"}}`), remoteVideoTokens, 0},
		{"video data URI", chat(`{"type":"video_url","video_url":{"url":"data:video/mp4;base64,` + b64(999_999) + `"}}`), 999_999/videoBytesPerToken + 1, 0},
		{"video in a file part", chat(`{"type":"file","file":{"filename":"a.mp4","file_data":"data:video/mp4;base64,` + b64(999_999) + `"}}`), 999_999/videoBytesPerToken + 1, 0},
		{"gemini inline video", gemini(`{"inlineData":{"mimeType":"video/mp4","data":"` + b64(999_999) + `"}}`), 999_999/videoBytesPerToken + 1, 0},
		{"gemini linked video", gemini(`{"fileData":{"mimeType":"video/mp4","fileUri":"https://example.com/a.mp4"}}`), remoteVideoTokens, 0},
		{"gemini linked image", gemini(`{"fileData":{"mimeType":"image/png","fileUri":"https://example.com/a.png"}}`), remoteImageTokens, 0},
		{"image data URI", chat(`{"type":"image_url","image_url":{"url":"data:image/png;base64,` + b64(300_000) + `"}}`), remoteImageTokens, 0},
		{"large image data URI", chat(`{"type":"image_url","image_url":{"url":"data:image/png;base64,` + b64(600_000) + `"}}`), imageBaseTokens + 600_000/imageBytesPerToken, 0},
		{"markdown image in text", `{"messages":[{"role":"user","content":"look ![x](data:image/png;base64,` + b64(3000) + `)"}]}`, remoteImageTokens, 0},
		{"compressed audio", chat(`{"type":"input_audio","input_audio":{"data":"` + b64(90_000) + `","format":"mp3"}}`), 0, 90_000/compressedAudioBytesPerToken + 1},
		{"pcm audio", chat(`{"type":"input_audio","input_audio":{"data":"` + b64(498_000) + `","format":"wav"}}`), 0, 498_000/pcmAudioBytesPerToken + 1},
		{"gemini inline audio", gemini(`{"inlineData":{"mimeType":"audio/ogg","data":"` + b64(9000) + `"}}`), 0, 9000/compressedAudioBytesPerToken + 1},
		{"pdf data URI", chat(`{"type":"file","file":{"filename":"a.pdf","file_data":"data:application/pdf;base64,` + b64(201_000) + `"}}`), imageBaseTokens + 201_000/documentBytesPerToken, 0},
		{"uploaded file id", chat(`{"type":"file","file":{"file_id":"file-abc"}}`), remoteDocumentTokens, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			usage := estimateUsage([]byte(tc.body), 10)
			assert.Equal(t, tc.audio, usage.PromptTokensDetails.AudioTokens)
			text := usage.PromptTokens - tc.visual - tc.audio
			assert.GreaterOrEqual(t, text, 1)
			assert.Less(t, text, 120, "media payload must not be priced as text")
			assert.Equal(t, usage.PromptTokens-tc.audio, usage.PromptTokensDetails.TextTokens)
			assert.Equal(t, 10, usage.CompletionTokens)
		})
	}
}

// TestVideoEstimateCoversGeminiVideoCost checks the inline video rate against
// Gemini's price of up to about 300 tokens per second, for bitrates from the
// lowest the rate covers to full HD.
func TestVideoEstimateCoversGeminiVideoCost(t *testing.T) {
	const seconds = 10
	for _, kbps := range []int{240, 500, 1000, 2500, 5000} {
		decoded := kbps * 1000 / 8 * seconds
		body := `{"contents":[{"parts":[{"inlineData":{"mimeType":"video/mp4","data":"` + strings.Repeat("A", decoded/3*4) + `"}}]}]}`
		usage := estimateUsage([]byte(body), 0)
		assert.GreaterOrEqual(t, usage.PromptTokens, seconds*300, "%d kbps", kbps)
	}
}

// ---------------------------------------------------------------------------
// Video batches
// ---------------------------------------------------------------------------

func TestOpenRouterVideoURLIsForwardedAndReservedAtTheAllowance(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	const videoURL = "https://cdn.example.com/clips/cat.mp4"
	file := upload(t, testUser, mediaLine("a", testModel, `{"type":"video_url","video_url":{"url":"`+videoURL+`"}}`, 200))

	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	assert.Contains(t, string(upstream.lastSubmit), videoURL, "OpenRouter fetches the video itself")
	reserved := reservedFor(t, upstream, file)
	assert.Equal(t, reserved, b.Quota)
	assert.GreaterOrEqual(t, b.Quota, expectedQuota(remoteVideoTokens, 200, 0.5))
	assert.Equal(t, startQuota-reserved, userQuota(t, testUser))

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 45_000, 150, "vendor/video-1", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	actual := expectedQuota(45_000, 150, 0.5)
	assert.Equal(t, actual, settled.Quota)
	assert.Equal(t, startQuota-actual, userQuota(t, testUser))
	assert.Equal(t, startQuota-actual, tokenRemain(t, testToken))
	log := lastLog(t)
	assert.Equal(t, model.LogTypeRefund, log.Type)
	assert.Equal(t, reserved-actual, log.Quota)
	assert.Equal(t, actual, usedQuota(t, testUser), "used quota ends at the charge")
}

func TestGeminiVideoURLIsDownloadedInlinedAndReservedBySize(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	video := fakeVideo(600_000)
	media := newMediaServer(t, video)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	file := upload(t, testUser, mediaLine("a", testModel, `{"type":"video_url","video_url":{"url":"`+media.url()+`"}}`, 200))

	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	assert.EqualValues(t, 1, media.hits.Load())
	parts := submittedGeminiParts(t, upstream)[0]
	require.Len(t, parts, 2)
	inline := parts[1]["inlineData"].(map[string]any)
	assert.Equal(t, "video/mp4", inline["mimeType"])
	assert.Equal(t, base64.StdEncoding.EncodeToString(video), inline["data"], "Gemini receives the downloaded video")
	assert.EqualValues(t, 200, submittedGenerationConfig(t, upstream, 0)["maxOutputTokens"])

	usage := submittedUsage(t, upstream, file, 100)[0]
	assert.GreaterOrEqual(t, usage.PromptTokens, len(video)/videoBytesPerToken, "the reservation follows the downloaded size")
	assert.Less(t, usage.PromptTokens, remoteVideoTokens, "a short clip is not reserved at the link allowance")
	assert.Equal(t, reservedFor(t, upstream, file), b.Quota)

	upstream.set(geminiSucceededWith(geminiMediaResult("a", 12, 2580, 0, 140)))
	settled := poll(t, b.BatchId)
	require.Equal(t, model.BatchStatusCompleted, settled.Status)
	actual := expectedQuota(2592, 140, 0.5)
	assert.Equal(t, actual, settled.Quota)
	assert.Equal(t, startQuota-actual, userQuota(t, testUser))
	assert.Equal(t, startQuota-actual, tokenRemain(t, testToken))
	assert.Equal(t, 2592, NewBatchObject(settled).Usage.InputTokens)
}

// TestGeminiVideoTokensBillAtTheTextPrice settles video batches of a model
// priced by its built-in expression: video tokens at the text price, a video's
// audio track at the audio price, at the batch ratio.
func TestGeminiVideoTokensBillAtTheTextPrice(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	const modelName = "gemini-3.1-flash-lite"
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	require.NoError(t, model.DB.Model(&model.Channel{}).Where("id = ?", 1).Update("models", modelName).Error)
	model.InitChannelCache()

	clip := base64.StdEncoding.EncodeToString(fakeVideo(300_000))
	file := upload(t, testUser,
		mediaLine("silent", modelName, `{"type":"video_url","video_url":{"url":"data:video/mp4;base64,`+clip+`"}}`, 100),
		mediaLine("talking", modelName, `{"type":"file","file":{"filename":"talk.mp4","file_data":"data:video/mp4;base64,`+clip+`"}}`, 100))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	for _, parts := range submittedGeminiParts(t, upstream) {
		require.Len(t, parts, 2)
		assert.Equal(t, "video/mp4", parts[1]["inlineData"].(map[string]any)["mimeType"])
	}

	// $0.25 text, image and video, $0.50 audio and $1.50 output per 1M tokens.
	price := func(text, audio, output int, ratio float64) int {
		return int(math.Round((float64(text)*0.25 + float64(audio)*0.5 + float64(output)*1.5) / 1e6 * 500000 * ratio))
	}
	reserved := 0
	for _, usage := range submittedUsage(t, upstream, file, 100) {
		assert.GreaterOrEqual(t, usage.PromptTokens, 300_000/videoBytesPerToken)
		reserved += price(usage.PromptTokens, usage.PromptTokensDetails.AudioTokens, usage.CompletionTokens, 0.5)
	}
	assert.Equal(t, reserved, b.Quota)

	upstream.set(geminiSucceededWith(geminiMediaResult("silent", 20, 5000, 0, 80), geminiMediaResult("talking", 20, 3000, 900, 60)))
	settled := poll(t, b.BatchId)
	require.Equal(t, model.BatchStatusCompleted, settled.Status)
	want := price(5020, 0, 80, 0.5) + price(3020, 900, 60, 0.5)
	assert.Equal(t, want, settled.Quota)
	assert.Equal(t, startQuota-want, userQuota(t, testUser))
	assert.Equal(t, 900, NewBatchObject(settled).Usage.InputTokensDetails["audio_tokens"])
}

func TestGeminiVideoBatchOverTheInlineLimitStopsDownloading(t *testing.T) {
	resetDB(t)
	allowMediaDownloads(t)
	media := newMediaServer(t, fakeVideo(8_400_000))
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	part := `{"type":"video_url","video_url":{"url":"` + media.url() + `"}}`
	file := upload(t, testUser, mediaLine("a", testModel, part, 50), mediaLine("b", testModel, part, 50), mediaLine("c", testModel, part, 50))

	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches")
	t.Cleanup(func() { service.CleanupFileSources(c) })
	_, apiErr := CreateBatch(c, &CreateRequest{InputFileId: file.FileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Status)
	assert.Equal(t, i18n.MsgBatchInlineTooLarge, apiErr.MsgID)
	assert.EqualValues(t, 1, media.hits.Load(), "a link repeated across requests is downloaded once")
	assert.Zero(t, upstream.submits)
	assert.Zero(t, countBatches(t))
	assert.Eventually(t, func() bool { return userQuota(t, testUser) == startQuota }, 2*time.Second, 10*time.Millisecond, "the reservation is refunded")
}

// ---------------------------------------------------------------------------
// Output limit
// ---------------------------------------------------------------------------

func TestBatchWithoutOutputLimitSendsTheDefaultAndStaysWithinItsReservation(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	line := fmt.Sprintf(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"write a long story"}]}}`, testModel)

	lines, apiErr := parseInput([]byte(line+"\n"), EndpointChatCompletions, 100)
	require.Nil(t, apiErr)
	body, err := lines[0].upstreamBody(testModel, 100)
	require.NoError(t, err)
	estimate := estimateUsage(body, 100)
	reservation := expectedQuota(estimate.PromptTokens, estimate.CompletionTokens, 0.5)
	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", testUser).Update("quota", reservation).Error)

	file := upload(t, testUser, line)
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr, "a balance equal to the reservation is enough")
	assert.Equal(t, reservation, b.Quota)
	assert.Zero(t, userQuota(t, testUser))
	var sent map[string]any
	require.NoError(t, json.Unmarshal(upstream.submittedBodies(t)[0], &sent))
	assert.EqualValues(t, 100, sent["max_tokens"], "the default output limit is sent upstream")

	// The upstream stops at the limit; the prompt is within its estimate.
	upstream.set(openRouterTerminal("completed", "["+orResult("a", estimate.PromptTokens, 100, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.LessOrEqual(t, settled.Quota, b.Quota)
	assert.Zero(t, userQuota(t, testUser), "the balance does not go negative")
}

func TestUpstreamBodyNormalizesOutputLimits(t *testing.T) {
	cases := []struct {
		name string
		body string
		want map[string]any
	}{
		{"none", `{"model":"x","messages":[1]}`, map[string]any{"max_tokens": 100.0}},
		{"own limit kept", `{"model":"x","messages":[1],"max_tokens":30}`, map[string]any{"max_tokens": 30.0}},
		{"zero completion limit dropped", `{"model":"x","messages":[1],"max_tokens":50,"max_completion_tokens":0}`, map[string]any{"max_tokens": 50.0}},
		{"completion limit kept", `{"model":"x","messages":[1],"max_completion_tokens":70}`, map[string]any{"max_completion_tokens": 70.0}},
		{"zero and null replaced", `{"model":"x","messages":[1],"max_tokens":0,"max_completion_tokens":null}`, map[string]any{"max_tokens": 100.0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines, apiErr := parseInput([]byte(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":`+tc.body+"}\n"), "", 10)
			require.Nil(t, apiErr)
			body, err := lines[0].upstreamBody("upstream-model", 100)
			require.NoError(t, err)
			var sent map[string]any
			require.NoError(t, json.Unmarshal(body, &sent))
			assert.Equal(t, "upstream-model", sent["model"])
			for _, field := range []string{"max_tokens", "max_completion_tokens"} {
				assert.Equal(t, tc.want[field], sent[field], field)
			}
		})
	}
}

func TestGeminiBatchWithoutOutputLimitSendsTheDefault(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	noLimit := fmt.Sprintf(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}]}}`, testModel)
	zeroCompletion := fmt.Sprintf(`{"custom_id":"b","method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}],"max_tokens":50,"max_completion_tokens":0}}`, testModel)
	file := upload(t, testUser, noLimit, zeroCompletion)
	_, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	assert.EqualValues(t, 100, submittedGenerationConfig(t, upstream, 0)["maxOutputTokens"])
	assert.EqualValues(t, 50, submittedGenerationConfig(t, upstream, 1)["maxOutputTokens"])
}

// ---------------------------------------------------------------------------
// Refunds move funds once
// ---------------------------------------------------------------------------

func TestRejectedSubmissionIsRefundedOnceWhenTheRecordCannotBeSettled(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	upstream.submitStatus = http.StatusInternalServerError
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	failing := failBatchWrites(t, false)

	file := upload(t, testUser, chatLine("a", "x", 50))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr)
	failing.Store(false)
	var left model.Batch
	require.NoError(t, model.DB.Where("user_id = ?", testUser).First(&left).Error)
	assert.Equal(t, model.BatchBillingReserved, left.BillingState)
	assert.Equal(t, startQuota-left.Quota, userQuota(t, testUser), "no refund while the record still holds the reservation")

	require.NoError(t, model.DB.Model(&model.Batch{}).Where("id = ?", left.Id).Update("created_at", time.Now().Add(-time.Hour).Unix()).Error)
	lost := poll(t, left.BatchId)
	assert.Equal(t, model.BatchBillingHeld, lost.BillingState)
	assert.Contains(t, lost.Errors, "submission_unconfirmed")
	assert.Equal(t, startQuota-left.Quota, userQuota(t, testUser))
	resolve(t, left.BatchId, ResolveActionRefund, nil)
	assert.Equal(t, startQuota, userQuota(t, testUser))
	assert.Equal(t, startQuota, tokenRemain(t, testToken))
	poll(t, left.BatchId)
	assert.Equal(t, startQuota, userQuota(t, testUser), "a second poll moves nothing")
	assert.Zero(t, usedQuota(t, testUser), "an unsubmitted batch is not counted as used")
}

func TestRejectedSubmissionIsRefundedOnceWhenTheRecordCannotBeDeleted(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	upstream.submitStatus = http.StatusInternalServerError
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	failBatchWrites(t, true)

	file := upload(t, testUser, chatLine("a", "x", 50))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr)
	assert.Equal(t, startQuota, userQuota(t, testUser))
	var left model.Batch
	require.NoError(t, model.DB.Where("user_id = ?", testUser).First(&left).Error)
	assert.Equal(t, model.BatchBillingSettled, left.BillingState)

	require.NoError(t, model.DB.Model(&model.Batch{}).Where("id = ?", left.Id).Updates(map[string]any{"created_at": time.Now().Add(-time.Hour).Unix(), "next_poll_at": 0}).Error)
	PollOnce(t.Context())
	assert.Equal(t, startQuota, userQuota(t, testUser), "the poller does not refund a settled batch")
	assert.Equal(t, startQuota, tokenRemain(t, testToken))
}

func TestClientDisconnectDoesNotAbortTheSubmission(t *testing.T) {
	resetDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if r.Method == http.MethodPost {
			cancel()
			time.Sleep(100 * time.Millisecond)
			_, _ = w.Write([]byte(`{"id":"orb-late","status":"validating"}`))
		}
	}))
	t.Cleanup(server.Close)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, server.URL)
	file := upload(t, testUser, chatLine("a", "x", 50))

	c, _ := requestContext(testUser, testToken, testTokenKey, http.MethodPost, "/v1/batches")
	c.Request = c.Request.WithContext(ctx)
	b, apiErr := CreateBatch(c, &CreateRequest{InputFileId: file.FileId, Endpoint: EndpointChatCompletions, CompletionWindow: CompletionWindow})
	require.Nil(t, apiErr)
	assert.Equal(t, "orb-late", b.UpstreamBatchId, "the batch the upstream accepted is tracked")
	assert.Equal(t, model.BatchBillingReserved, b.BillingState)
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser))
}

func TestUnsavedSubmissionIsCancelledUpstreamAndRefundedOnce(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	upstream.set(`{"name":"batches/g1","metadata":{"state":"BATCH_STATE_RUNNING"}}`)
	seedBatchChannel(t, 1, constant.ChannelTypeGemini, upstream.server.URL)
	failing := failBatchWrites(t, false)

	file := upload(t, testUser, chatLine("a", "x", 50))
	_, apiErr := create(t, file.FileId)
	require.NotNil(t, apiErr)
	assert.Equal(t, 1, upstream.submits)
	assert.Equal(t, 1, upstream.cancels, "the upstream batch is not left running unbilled")
	failing.Store(false)

	var left model.Batch
	require.NoError(t, model.DB.Where("user_id = ?", testUser).First(&left).Error)
	require.Empty(t, left.UpstreamBatchId)
	require.NoError(t, model.DB.Model(&model.Batch{}).Where("id = ?", left.Id).Update("created_at", time.Now().Add(-time.Hour).Unix()).Error)
	held := poll(t, left.BatchId)
	assert.Equal(t, model.BatchBillingHeld, held.BillingState, "the cancelled upstream batch may still bill what it ran")
	resolve(t, left.BatchId, ResolveActionRefund, nil)
	assert.Equal(t, startQuota, userQuota(t, testUser))
	assert.Equal(t, startQuota, tokenRemain(t, testToken))
	assert.Zero(t, usedQuota(t, testUser))
}

func TestLostSubmissionRefundsTheSubscriptionOnce(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	sub := &model.UserSubscription{Id: 9, UserId: testUser, AmountTotal: 1_000_000, AmountUsed: 1500, Status: "active",
		StartTime: time.Now().Unix(), EndTime: time.Now().Add(time.Hour).Unix()}
	require.NoError(t, model.DB.Create(sub).Error)
	b := &model.Batch{BatchId: "batch_sub_lost", UserId: testUser, TokenId: testToken, ChannelId: 1, ChannelType: constant.ChannelTypeOpenRouter,
		Status: model.BatchStatusValidating, Quota: 500, BillingState: model.BatchBillingReserved,
		BillingSource: "subscription", SubscriptionId: 9, Model: testModel, CreatedAt: time.Now().Add(-time.Hour).Unix(),
		SubmissionUnrecorded: true}
	require.NoError(t, model.CreateBatch(b))

	assert.Equal(t, model.BatchBillingHeld, poll(t, b.BatchId).BillingState)
	resolve(t, b.BatchId, ResolveActionRefund, nil)
	resolveErr(t, b.BatchId, ResolveActionRefund)
	var reloaded model.UserSubscription
	require.NoError(t, model.DB.First(&reloaded, 9).Error)
	assert.Equal(t, int64(1000), reloaded.AmountUsed)
	assert.Equal(t, startQuota, userQuota(t, testUser), "the wallet is untouched")
	assert.Zero(t, usedQuota(t, testUser))
}

// ---------------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------------

func TestBatchWithoutUpstreamExpiryIsHeldAfterItsWindow(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 50))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	require.NoError(t, model.DB.Exec("DELETE FROM channels").Error)
	require.NoError(t, model.DB.Exec("DELETE FROM abilities").Error)
	model.InitChannelCache()

	recent := time.Now().Add(-2 * time.Hour).Unix()
	require.NoError(t, model.DB.Model(&model.Batch{}).Where("batch_id = ?", b.BatchId).Updates(map[string]any{"expires_at": 0, "created_at": recent}).Error)
	waiting := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingReserved, waiting.BillingState, "within its window the batch is retried")

	old := time.Now().Add(-(completionWindowDuration + upstreamLostAfter + time.Hour)).Unix()
	require.NoError(t, model.DB.Model(&model.Batch{}).Where("batch_id = ?", b.BatchId).Update("created_at", old).Error)
	held := poll(t, b.BatchId)
	assert.Equal(t, model.BatchBillingHeld, held.BillingState)
	assert.Contains(t, held.Errors, "channel_unavailable")
	assert.Equal(t, startQuota-b.Quota, userQuota(t, testUser), "the upstream may have billed it")

	charged := resolve(t, b.BatchId, ResolveActionCharge, common.GetPointer(30))
	assert.Equal(t, model.BatchStatusFailed, charged.Status)
	assert.Equal(t, 30, charged.Quota)
	assert.Equal(t, startQuota-30, userQuota(t, testUser), "the administrator charges what the upstream billed")
	assert.Equal(t, startQuota-30, tokenRemain(t, testToken))
	assert.Equal(t, 30, usedQuota(t, testUser))
}

func TestUpstreamNotFoundRecoversWhenTheBatchReappears(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)

	upstream.mu.Lock()
	upstream.retrieveCode = http.StatusNotFound
	upstream.mu.Unlock()
	poll(t, b.BatchId)
	poll(t, b.BatchId)
	upstream.set(`{"id":"orb-1","status":"in_progress","request_counts":{"total":1,"completed":0,"failed":0}}`)
	running := poll(t, b.BatchId)
	assert.Equal(t, model.BatchStatusInProgress, running.Status)
	assert.Zero(t, running.PollFailures)
	assert.Equal(t, model.BatchBillingReserved, running.BillingState)

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 3, 3, "m", 0)+"]", "0"))
	assert.Equal(t, expectedQuota(3, 3, 0.5), poll(t, b.BatchId).Quota)
}

func TestExpiredInputFileIsKeptUntilItsBatchSettles(t *testing.T) {
	resetDB(t)
	upstream := newFakeUpstream(t)
	seedBatchChannel(t, 1, constant.ChannelTypeOpenRouter, upstream.server.URL)
	file := upload(t, testUser, chatLine("a", "x", 10), chatLine("b", "y", 10))
	b, apiErr := create(t, file.FileId)
	require.Nil(t, apiErr)
	require.NoError(t, model.DB.Model(&model.BatchFile{}).Where("file_id = ?", file.FileId).Update("expires_at", 1).Error)
	assert.True(t, model.HasExpiredBatchFiles(common.GetTimestamp()))

	upstream.set(`{"id":"orb-1","status":"in_progress","request_counts":{"total":2,"completed":1,"failed":0}}`)
	poll(t, b.BatchId)
	_, err := model.GetBatchFile(testUser, file.FileId, false)
	require.NoError(t, err, "the input of an unsettled batch survives its retention")

	upstream.set(openRouterTerminal("completed", "["+orResult("a", 4, 2, "m", 0)+","+orResult("b", 6, 2, "m", 0)+"]", "0"))
	settled := poll(t, b.BatchId)
	assert.Equal(t, 2, settled.RequestCompleted, "settlement still reads the input")
	_, err = model.GetBatchFile(testUser, file.FileId, false)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "deleted once the batch settled")
	_, err = model.GetBatchFile(testUser, settled.OutputFileId, false)
	assert.NoError(t, err)
}
