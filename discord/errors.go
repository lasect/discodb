package discord

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bwmarrin/discordgo"
)

// Sentinel errors for Discord API operations.
var (
	ErrNotFound        = errors.New("resource not found")
	ErrRateLimited     = errors.New("rate limited")
	ErrForbidden       = errors.New("forbidden")
	ErrUnauthorized    = errors.New("unauthorized")
	ErrBadRequest      = errors.New("bad request")
	ErrServerError     = errors.New("discord server error")
	ErrTransport       = errors.New("transport error")
	ErrInvalidResponse = errors.New("invalid response")
	ErrChannelNotFound = errors.New("channel not found")
	ErrMessageNotFound = errors.New("message not found")
	ErrGuildNotFound   = errors.New("guild not found")
	ErrRoleNotFound    = errors.New("role not found")
)

// APIError represents a structured Discord API error with context.
type APIError struct {
	Op         string        // Operation that failed (e.g., "SendMessage", "GetChannel")
	StatusCode int           // HTTP status code
	Message    string        // Error message from Discord
	Code       int           // Discord error code
	RetryAfter time.Duration // For rate limits, how long to wait
	Err        error         // Underlying error
}

func (e *APIError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%s: %s (status=%d, code=%d, retry_after=%s)",
			e.Op, e.Message, e.StatusCode, e.Code, e.RetryAfter)
	}
	return fmt.Sprintf("%s: %s (status=%d, code=%d)", e.Op, e.Message, e.StatusCode, e.Code)
}

func (e *APIError) Unwrap() error {
	return e.Err
}

// Is implements errors.Is for APIError.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrNotFound:
		return e.StatusCode == http.StatusNotFound
	case ErrRateLimited:
		return e.StatusCode == http.StatusTooManyRequests
	case ErrForbidden:
		return e.StatusCode == http.StatusForbidden
	case ErrUnauthorized:
		return e.StatusCode == http.StatusUnauthorized
	case ErrBadRequest:
		return e.StatusCode == http.StatusBadRequest
	case ErrServerError:
		return e.StatusCode >= 500
	}
	return false
}

// IsRetryable returns true if the error is transient and the operation can be retried.
func (e *APIError) IsRetryable() bool {
	switch e.StatusCode {
	case http.StatusTooManyRequests:
		return true
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return true
	case 0:
		// Transport/network errors are retryable
		return errors.Is(e.Err, ErrTransport)
	}
	return false
}

// wrapError normalizes discordgo errors into APIError.
func wrapError(op string, err error) error {
	if err == nil {
		return nil
	}

	apiErr := &APIError{
		Op:  op,
		Err: err,
	}

	// Handle discordgo RESTError
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) {
		apiErr.StatusCode = restErr.Response.StatusCode
		if restErr.Message != nil {
			apiErr.Message = restErr.Message.Message
			apiErr.Code = restErr.Message.Code
		} else {
			apiErr.Message = restErr.Response.Status
		}

		// Extract retry-after header for rate limits
		if apiErr.StatusCode == http.StatusTooManyRequests {
			if retryAfter := restErr.Response.Header.Get("Retry-After"); retryAfter != "" {
				if secs, parseErr := strconv.ParseFloat(retryAfter, 64); parseErr == nil {
					apiErr.RetryAfter = time.Duration(secs * float64(time.Second))
				}
			}
			apiErr.Err = ErrRateLimited
		}

		// Map to sentinel errors
		switch apiErr.StatusCode {
		case http.StatusNotFound:
			apiErr.Err = ErrNotFound
		case http.StatusForbidden:
			apiErr.Err = ErrForbidden
		case http.StatusUnauthorized:
			apiErr.Err = ErrUnauthorized
		case http.StatusBadRequest:
			apiErr.Err = ErrBadRequest
		}

		return apiErr
	}

	// Handle rate limit errors from discordgo
	var rateLimitErr *discordgo.RateLimitError
	if errors.As(err, &rateLimitErr) {
		apiErr.StatusCode = http.StatusTooManyRequests
		apiErr.Message = rateLimitErr.Message
		apiErr.RetryAfter = rateLimitErr.RetryAfter
		apiErr.Err = ErrRateLimited
		return apiErr
	}

	// Generic error - likely transport/network issue
	apiErr.Message = err.Error()
	apiErr.Err = ErrTransport
	return apiErr
}

// IsNotFound returns true if the error indicates a resource was not found.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsRateLimited returns true if the error indicates rate limiting.
func IsRateLimited(err error) bool {
	return errors.Is(err, ErrRateLimited)
}

// IsRetryable returns true if the error is transient and can be retried.
func IsRetryable(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}
	return false
}

// RetryAfter returns the retry-after duration for rate limit errors.
func RetryAfter(err error) time.Duration {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.RetryAfter
	}
	return 0
}
