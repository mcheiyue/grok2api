package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
)

// streamPrimeTimeout is how long we wait for the first *valid SSE data event*
// after a 2xx response. Slow first-token (reasoning) can exceed 30s; keep
// aligned with typical client timeouts without waiting forever.
const streamPrimeTimeout = 90 * time.Second

// maxPrimeHeadBytes caps how much we buffer while hunting for a valid event.
const maxPrimeHeadBytes = 64 << 10

var (
	errStreamPrimeEmpty   = errors.New("upstream stream ended before first byte")
	errStreamPrimeNoEvent = errors.New("upstream stream ended before first valid SSE data event")
)

// streamPrimeError carries how many head bytes were seen before prime failed.
type streamPrimeError struct {
	Bytes int
	Err   error
}

func (e *streamPrimeError) Error() string {
	if e == nil || e.Err == nil {
		return "upstream stream prime failed"
	}
	return fmt.Sprintf("%v (prime_bytes=%d)", e.Err, e.Bytes)
}

func (e *streamPrimeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

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

// primeStreamingBody blocks until the upstream body yields at least one
// complete SSE event with a non-empty JSON `data:` payload (or fails/times out).
// Keepalive comments (`: ...`) and bare whitespace do NOT count — that is the
// fix for 200warn cases where "any 1 byte" released the body too early.
//
// On success the returned ReadCloser replays the buffered head so the caller
// can hand the body to the client unchanged. On failure the original body is closed.
//
// The second return value (downgraded) is true when the body was treated as
// non-SSE (e.g. a large JSON blob with Content-Type: text/event-stream) and
// the caller should correct the Content-Type header to application/json.
func primeStreamingBody(body io.ReadCloser, timeout time.Duration) (io.ReadCloser, bool, error) {
	if body == nil {
		return nil, false, &streamPrimeError{Err: errStreamPrimeEmpty}
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
		var head bytes.Buffer
		buf := make([]byte, 8192)
		for {
			n, err := body.Read(buf)
			if n > 0 {
				// Avoid unbounded growth if the peer never sends a valid event.
				if head.Len()+n > maxPrimeHeadBytes {
					ch <- readResult{
						data: head.Bytes(),
						err:  fmt.Errorf("upstream stream prime head exceeded %d bytes without valid SSE event", maxPrimeHeadBytes),
					}
					return
				}
				_, _ = head.Write(buf[:n])
				if hasValidSSEDataEvent(head.Bytes()) {
					ch <- readResult{data: append([]byte(nil), head.Bytes()...), err: nil}
					return
				}
			}
			if err != nil {
				ch <- readResult{data: append([]byte(nil), head.Bytes()...), err: err}
				return
			}
		}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case res := <-ch:
		if hasValidSSEDataEvent(res.data) && (res.err == nil || errors.Is(res.err, io.EOF)) {
			// Valid event in head; keep body open for the remainder (may already be EOF).
			return &primedBody{
				reader: io.MultiReader(bytes.NewReader(res.data), body),
				closer: body,
			}, false, nil
		}
		primeBytes := len(res.data)
		// If the upstream sent significant data without valid SSE events, the
		// response is likely non-SSE (e.g. a large JSON blob). Pass it through
		// rather than failing and retrying. 4 KiB is a safe threshold: real
		// responses will exceed it, keepalive/comment noise will not.
		if primeBytes >= 4096 {
			_ = body.Close()
			return &primedBody{
				reader: bytes.NewReader(res.data),
				closer: io.NopCloser(nil),
			}, true, nil
		}
		_ = body.Close()
		if primeBytes == 0 {
			if res.err == nil || errors.Is(res.err, io.EOF) {
				return nil, false, &streamPrimeError{Bytes: 0, Err: errStreamPrimeEmpty}
			}
			return nil, false, &streamPrimeError{Bytes: 0, Err: res.err}
		}
		if res.err == nil || errors.Is(res.err, io.EOF) {
			return nil, false, &streamPrimeError{Bytes: primeBytes, Err: errStreamPrimeNoEvent}
		}
		return nil, false, &streamPrimeError{Bytes: primeBytes, Err: res.err}
	case <-timer.C:
		_ = body.Close()
		// Allow the blocked Read to unblock after Close without leaking the goroutine.
		go func() { <-ch }()
		return nil, false, &streamPrimeError{
			Bytes: 0,
			Err:   fmt.Errorf("upstream stream prime timeout after %s", timeout),
		}
	}
}

// hasValidSSEDataEvent reports whether data contains at least one complete SSE
// event (terminated by a blank line) with a non-empty JSON `data:` payload.
// Keepalive comments and empty data lines do not qualify.
func hasValidSSEDataEvent(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	// Walk complete events ending at \n\n (accept \r\n\r\n too via normalize).
	normalized := bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
	start := 0
	for start < len(normalized) {
		rel := bytes.Index(normalized[start:], []byte("\n\n"))
		if rel < 0 {
			return false
		}
		event := normalized[start : start+rel]
		start += rel + 2
		if sseEventHasJSONData(event) {
			return true
		}
	}
	return false
}

func sseEventHasJSONData(event []byte) bool {
	for _, rawLine := range bytes.Split(event, []byte("\n")) {
		line := bytes.TrimSpace(rawLine)
		if len(line) == 0 || line[0] == ':' {
			continue // empty or comment
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		// Real upstream events are JSON objects/arrays.
		if payload[0] == '{' || payload[0] == '[' {
			return true
		}
	}
	return false
}

// primeBytesOf extracts buffered head size from a prime error (0 if unknown).
func primeBytesOf(err error) int {
	var pe *streamPrimeError
	if errors.As(err, &pe) && pe != nil {
		return pe.Bytes
	}
	return 0
}
