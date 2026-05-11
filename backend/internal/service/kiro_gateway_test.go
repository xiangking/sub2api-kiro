package service

import (
	"net/http"
	"testing"
)

func TestNormalizeKiroUpstreamStatusTreatsCapacity400AsRateLimit(t *testing.T) {
	body := []byte(`{"__type":"com.amazon.aws.codewhisperer#ThrottlingException","message":"I am experiencing high traffic, please try again shortly.","reason":"INSUFFICIENT_MODEL_CAPACITY"}`)
	if got := normalizeKiroUpstreamStatus(http.StatusBadRequest, body); got != http.StatusTooManyRequests {
		t.Fatalf("normalizeKiroUpstreamStatus = %d, want %d", got, http.StatusTooManyRequests)
	}
}

func TestNormalizeKiroUpstreamStatusKeepsValidation400(t *testing.T) {
	body := []byte(`{"__type":"com.amazon.aws.codewhisperer#ValidationException","message":"Improperly formed request."}`)
	if got := normalizeKiroUpstreamStatus(http.StatusBadRequest, body); got != http.StatusBadRequest {
		t.Fatalf("normalizeKiroUpstreamStatus = %d, want %d", got, http.StatusBadRequest)
	}
}
