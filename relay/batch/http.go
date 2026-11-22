package batch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/service"
)

// maxUpstreamResponseBytes bounds a batch response read into memory; inline
// results of a full batch fit well below it.
const maxUpstreamResponseBytes = 512 << 20

func doUpstream(ctx context.Context, conn *Connection, method string, url string, headers map[string]string, body []byte) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	client, err := service.GetHttpClientWithProxy(conn.Proxy)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxUpstreamResponseBytes {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", maxUpstreamResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &UpstreamError{StatusCode: resp.StatusCode, Message: upstreamErrorMessage(data)}
	}
	return data, nil
}

// upstreamErrorMessage extracts error.message from an error body, falling
// back to a bounded prefix of the raw body.
func upstreamErrorMessage(data []byte) string {
	var parsed struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if common.Unmarshal(data, &parsed) == nil {
		if parsed.Error.Message != "" {
			return parsed.Error.Message
		}
		if parsed.Message != "" {
			return parsed.Message
		}
	}
	text := strings.TrimSpace(string(data))
	if len(text) > 300 {
		text = text[:300]
	}
	return text
}
