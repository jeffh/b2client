package b2

import (
	"errors"
	"fmt"
	"time"
)

var ErrAuthTokenMissing = errors.New("auth token is required")

func IsTimeoutErr(err error) bool {
	type timeoutErr interface {
		error
		Timeout() bool
	}

	if err, ok := err.(timeoutErr); ok && err.Timeout() {
		return true
	}
	return false
}

type ErrorResponse struct {
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`

	// typically set if IsTooManyRequests() == true
	RetryAfter time.Duration `json:"-"`
}

func (e *ErrorResponse) IsBadRequest() bool         { return e.Status == 400 }
func (e *ErrorResponse) IsUnauthorized() bool       { return e.Status == 401 }
func (e *ErrorResponse) IsForbidden() bool          { return e.Status == 403 }
func (e *ErrorResponse) IsRequestTimeout() bool     { return e.Status == 408 }
func (e *ErrorResponse) IsTooManyRequests() bool    { return e.Status == 429 }
func (e *ErrorResponse) IsInternalError() bool      { return e.Status == 500 }
func (e *ErrorResponse) IsServiceUnavailable() bool { return e.Status == 503 }

func (e *ErrorResponse) Timeout() bool {
	return e.IsRequestTimeout() || e.IsTooManyRequests()
}

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("%d: %s - %s", e.Status, e.Code, e.Message)
}

const (
	ErrCodeBadRequest          = "bad_request"
	ErrCodeUnauthorized        = "unauthorized"
	ErrCodeBadAuthToken        = "bad_auth_token"
	ErrCodeExpiredAuthToken    = "expired_auth_token"
	ErrCodeDownloadCapExceeded = "download_cap_exceeded"
	ErrCodeNotFound            = "not_found"
	ErrCodeRangeNotSatisfiable = "range_not_satisfiable"
)
