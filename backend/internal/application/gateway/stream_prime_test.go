package gateway

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type stallReader struct {
	delay time.Duration
	data  string
	err   error
}

func (s *stallReader) Read(p []byte) (int, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
		s.delay = 0
	}
	if s.data == "" {
		if s.err != nil {
			return 0, s.err
		}
		return 0, io.EOF
	}
	n := copy(p, s.data)
	s.data = s.data[n:]
	if len(s.data) == 0 {
		if s.err != nil {
			return n, s.err
		}
		return n, io.EOF
	}
	return n, nil
}

func (s *stallReader) Close() error { return nil }

func TestPrimeStreamingBodyReplaysValidSSE(t *testing.T) {
	t.Parallel()
	src := io.NopCloser(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\nrest"))
	primed, downgraded, err := primeStreamingBody(src, time.Second)
	if err != nil {
		t.Fatalf("prime: %v", err)
	}
	if downgraded {
		t.Fatal("unexpected downgraded for valid SSE")
	}
	defer primed.Close()
	all, err := io.ReadAll(primed)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if got := string(all); got != "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\nrest" {
		t.Fatalf("got %q", got)
	}
}

func TestPrimeStreamingBodyRejectsKeepaliveOnly(t *testing.T) {
	t.Parallel()
	// Classic false prime: comment/keepalive bytes then EOF — must NOT succeed.
	src := io.NopCloser(strings.NewReader(": keep-alive\n\n"))
	_, _, err := primeStreamingBody(src, time.Second)
	if !errors.Is(err, errStreamPrimeNoEvent) {
		t.Fatalf("want errStreamPrimeNoEvent, got %v", err)
	}
	if primeBytesOf(err) == 0 {
		t.Fatalf("expected prime_bytes > 0, err=%v", err)
	}
}

func TestPrimeStreamingBodyWaitsPastKeepaliveForJSON(t *testing.T) {
	t.Parallel()
	src := io.NopCloser(strings.NewReader(": ping\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\nmore"))
	primed, downgraded, err := primeStreamingBody(src, time.Second)
	if err != nil {
		t.Fatalf("prime: %v", err)
	}
	if downgraded {
		t.Fatal("unexpected downgraded for valid SSE")
	}
	defer primed.Close()
	all, _ := io.ReadAll(primed)
	if !strings.Contains(string(all), `"response.output_text.delta"`) || !strings.HasSuffix(string(all), "more") {
		t.Fatalf("got %q", all)
	}
}

func TestPrimeStreamingBodyRejectsMetadataBeforePayload(t *testing.T) {
	t.Parallel()
	src := io.NopCloser(strings.NewReader("data: {\"type\":\"response.created\"}\n\n"))
	_, _, err := primeStreamingBody(src, time.Second)
	if !errors.Is(err, errStreamPrimeNoEvent) {
		t.Fatalf("want metadata-only stream rejected, got %v", err)
	}
}

func TestPrimeStreamingBodyEmptyEOF(t *testing.T) {
	t.Parallel()
	src := io.NopCloser(strings.NewReader(""))
	_, _, err := primeStreamingBody(src, time.Second)
	if !errors.Is(err, errStreamPrimeEmpty) {
		t.Fatalf("want errStreamPrimeEmpty, got %v", err)
	}
}

func TestPrimeStreamingBodyTimeout(t *testing.T) {
	t.Parallel()
	src := &stallReader{delay: 200 * time.Millisecond, data: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"}
	_, _, err := primeStreamingBody(src, 20*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "prime timeout") {
		t.Fatalf("want prime timeout, got %v", err)
	}
}

func TestPrimeStreamingBodyReadError(t *testing.T) {
	t.Parallel()
	boom := errors.New("conn reset")
	src := &stallReader{err: boom}
	_, _, err := primeStreamingBody(src, time.Second)
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestHasValidSSEDataEvent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{": ping\n\n", false},
		{"data: \n\n", false},
		{"data: [DONE]\n\n", false},
		{"data: {\"type\":\"response.created\"}\n\n", false},
		{"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n", true},
		{": ping\n\ndata: {\"a\":1}\n\n", false},
		{"data: {\"type\":\"response.created\"}\n", false}, // incomplete event
		{"event: message\ndata: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"x\"}\n\n", true},
	}
	for _, tc := range cases {
		if got := hasValidSSEDataEvent([]byte(tc.in)); got != tc.want {
			t.Fatalf("in=%q got=%v want=%v", tc.in, got, tc.want)
		}
	}
}
