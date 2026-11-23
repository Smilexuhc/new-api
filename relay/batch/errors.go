package batch

import (
	"errors"

	"github.com/QuantumNous/new-api/i18n"
	"github.com/gin-gonic/gin"
)

var ErrGeminiInlineTooLarge = errors.New("the batch exceeds the Gemini inline request limit")

// Error is a client-facing batch API error. Message is set when the text is
// already final (for example a billing error); otherwise MsgID is translated
// for the request.
type Error struct {
	Status  int
	Code    string
	MsgID   string
	Params  map[string]any
	Message string
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.MsgID
}

func (e *Error) Text(c *gin.Context) string {
	if e.Message != "" {
		return e.Message
	}
	return i18n.T(c, e.MsgID, e.Params)
}

func newError(status int, code string, msgID string, params map[string]any) *Error {
	return &Error{Status: status, Code: code, MsgID: msgID, Params: params}
}
