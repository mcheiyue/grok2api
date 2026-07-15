package gateway

import (
	"net/http"
	"testing"
)

// W2.3：钉死与 Gork 对齐的失败分类语义（chen failure.go 已具备，本测防回退）。
func TestUpstreamFailureClassifiesAuthQuotaRateLimit(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		check    func(*testing.T, *UpstreamFailure)
	}{
		{
			name:   "401 credential rejected",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"unauthorized","message":"bad token"}}`,
			check: func(t *testing.T, f *UpstreamFailure) {
				if !f.CredentialRejected || !f.AccountScoped || f.Code != "upstream_unauthorized" {
					t.Fatalf("%#v", f)
				}
			},
		},
		{
			name:   "403 chat endpoint denied permanent",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"permission-denied","message":"Access to the chat endpoint is denied"}}`,
			check: func(t *testing.T, f *UpstreamFailure) {
				if !f.PermanentAccountDenial || !f.AccountScoped {
					t.Fatalf("expected permanent denial: %#v", f)
				}
			},
		},
		{
			name:   "429 account scoped rate limit",
			status: http.StatusTooManyRequests,
			body:   `{"error":{"message":"Requests per Minute (actual/limit): 80/60"}}`,
			check: func(t *testing.T, f *UpstreamFailure) {
				if !f.AccountScoped || f.Code != "upstream_rate_limited" {
					t.Fatalf("%#v", f)
				}
			},
		},
		{
			name:   "402 quota exhausted",
			status: http.StatusPaymentRequired,
			body:   `{"error":{"message":"payment required"}}`,
			check: func(t *testing.T, f *UpstreamFailure) {
				if !f.QuotaExhausted || !f.AccountScoped {
					t.Fatalf("%#v", f)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newHTTPUpstreamFailure(tc.status, []byte(tc.body), 1, "acc")
			if f == nil {
				t.Fatal("nil failure")
			}
			tc.check(t, f)
		})
	}
}
