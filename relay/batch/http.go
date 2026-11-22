package batch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/service"
)

// maxUpstreamResponseBytes bounds a batch response read into memory; inline
// results of a full batch fit well below it.
const maxUpstreamResponseBytes = 512 << 20

// maxMessageLength bounds an upstream message kept in a batch or shown to the
// user.
const maxMessageLength = 1000

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
		if requestNeverSent(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", errResponseLost, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errResponseLost, err)
	}
	if len(data) > maxUpstreamResponseBytes {
		return nil, fmt.Errorf("%w: upstream response exceeds %d bytes", errResponseLost, maxUpstreamResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &UpstreamError{StatusCode: resp.StatusCode, Message: cleanMessage(upstreamErrorMessage(data), conn.BaseURL)}
	}
	return data, nil
}

// errResponseLost marks a request that may have reached the upstream but
// whose response was not read.
var errResponseLost = errors.New("the upstream response was lost")

// requestNeverSent reports whether a transport error happened before the
// request could reach the upstream.
func requestNeverSent(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && (opErr.Op == "dial" || opErr.Op == "proxyconnect")
}

// upstreamErrorMessage extracts error.message from an error body, falling
// back to the raw body.
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
	return string(data)
}

// maskedAddressPort is a port left on an address the shared masker already
// starred, as in "***.***.***.***:3590", "***.com:8443" or "[***]:443".
var maskedAddressPort = regexp.MustCompile(`(\*\*\*(?:\.\*\*\*)*(?:\.[A-Za-z]{2,24}){0,3}|\]):\d{2,5}\b`)

// bareHostPattern is a hostname, with an optional port, that the shared masker
// leaves alone because it is not the channel's base URL. The prefix is kept.
var bareHostPattern = regexp.MustCompile(`(^|[^A-Za-z0-9.-])((?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,24})(?::\d{1,5})?\b`)

// cleanMessage masks addresses and keys in an upstream message, as the
// synchronous relay does, then also hides ports and hostnames that appear on
// their own, and bounds its length.
func cleanMessage(message string, upstreamURLs ...string) string {
	message = common.MaskSensitiveInfo(strings.TrimSpace(message), upstreamURLs...)
	message = bareHostPattern.ReplaceAllString(message, "${1}***")
	message = maskedAddressPort.ReplaceAllString(message, "${1}:***")
	if len(message) > maxMessageLength {
		message = strings.ToValidUTF8(message[:maxMessageLength], "") + "..."
	}
	return message
}
