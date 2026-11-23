package service

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountRequestTokenDoesNotDownloadVideos(t *testing.T) {
	gin.SetMode(gin.TestMode)
	savedMedia, savedNotStream := constant.GetMediaToken, constant.GetMediaTokenNotStream
	constant.GetMediaToken, constant.GetMediaTokenNotStream = true, true
	t.Cleanup(func() { constant.GetMediaToken, constant.GetMediaTokenNotStream = savedMedia, savedNotStream })

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	body := `{"model":"gpt-4o","messages":[{"role":"user","content":[` +
		`{"type":"text","text":"describe"},` +
		`{"type":"video_url","video_url":{"url":"` + server.URL + `/clip.mp4"}}]}]}`
	var req dto.GeneralOpenAIRequest
	require.NoError(t, common.Unmarshal([]byte(body), &req))

	for _, stream := range []bool{false, true} {
		meta := req.GetTokenCountMeta()
		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		info := &relaycommon.RelayInfo{RelayFormat: types.RelayFormatOpenAI, IsStream: stream}
		tokens, err := CountRequestToken(ctx, meta, info)
		require.NoError(t, err, "stream=%v", stream)
		assert.Greater(t, tokens, 8192, "stream=%v: the video counts its fixed tokens", stream)
	}
	assert.Zero(t, hits.Load(), "the video is not downloaded")
}
