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

func TestPrimeStreamingBodyReplaysHead(t *testing.T) {
	t.Parallel()
	src := io.NopCloser(strings.NewReader("data: hello\n\nrest"))
	primed, err := primeStreamingBody(src, time.Second)
	if err != nil {
		t.Fatalf("prime: %v", err)
	}
	defer primed.Close()
	all, err := io.ReadAll(primed)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if got := string(all); got != "data: hello\n\nrest" {
		t.Fatalf("got %q", got)
	}
}

func TestPrimeStreamingBodyEmptyEOF(t *testing.T) {
	t.Parallel()
	src := io.NopCloser(strings.NewReader(""))
	_, err := primeStreamingBody(src, time.Second)
	if !errors.Is(err, errStreamPrimeEmpty) {
		t.Fatalf("want errStreamPrimeEmpty, got %v", err)
	}
}

func TestPrimeStreamingBodyTimeout(t *testing.T) {
	t.Parallel()
	src := &stallReader{delay: 200 * time.Millisecond, data: "late"}
	_, err := primeStreamingBody(src, 20*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "prime timeout") {
		t.Fatalf("want prime timeout, got %v", err)
	}
}

func TestPrimeStreamingBodyReadError(t *testing.T) {
	t.Parallel()
	boom := errors.New("conn reset")
	src := &stallReader{err: boom}
	_, err := primeStreamingBody(src, time.Second)
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}
