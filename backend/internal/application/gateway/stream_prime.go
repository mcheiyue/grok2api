package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
)

// streamPrimeTimeout is how long we wait for the first upstream stream byte
// after a 2xx response. Slow first-token (reasoning) can exceed 30s; keep
// aligned with typical client timeouts without waiting forever.
const streamPrimeTimeout = 90 * time.Second

var errStreamPrimeEmpty = errors.New("upstream stream ended before first byte")

// primedBody replays the prefetched head, then continues reading from source.
type primedBody struct {
	reader io.Reader
	closer io.Closer
}

func (p *primedBody) Read(buffer []byte) (int, error) {
	return p.reader.Read(buffer)
}

func (p *primedBody) Close() error {
	if p == nil || p.closer == nil {
		return nil
	}
	return p.closer.Close()
}

// primeStreamingBody blocks until the upstream body yields at least one byte
// (or fails/times out). On success the returned ReadCloser replays those bytes
// so the caller can hand the body to the client unchanged. On failure the
// original body is closed.
//
// This is the Option-A preflight for 200warn / upstream_stream_interrupted:
// HTTP 2xx headers arrived but the SSE body died before any payload — retry
// is still safe because nothing was written to the client yet.
func primeStreamingBody(body io.ReadCloser, timeout time.Duration) (io.ReadCloser, error) {
	if body == nil {
		return nil, errStreamPrimeEmpty
	}
	if timeout <= 0 {
		timeout = streamPrimeTimeout
	}

	type readResult struct {
		data []byte
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 8192)
		n, err := body.Read(buf)
		var data []byte
		if n > 0 {
			data = append([]byte(nil), buf[:n]...)
		}
		ch <- readResult{data: data, err: err}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case res := <-ch:
		if len(res.data) == 0 {
			_ = body.Close()
			if res.err == nil || errors.Is(res.err, io.EOF) {
				return nil, errStreamPrimeEmpty
			}
			return nil, res.err
		}
		// Prefetched head + remainder (source may already be at EOF).
		return &primedBody{
			reader: io.MultiReader(bytes.NewReader(res.data), body),
			closer: body,
		}, nil
	case <-timer.C:
		_ = body.Close()
		// Allow the blocked Read to unblock after Close without leaking the goroutine.
		go func() { <-ch }()
		return nil, fmt.Errorf("upstream stream prime timeout after %s", timeout)
	}
}
