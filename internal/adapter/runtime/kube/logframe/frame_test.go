package logframe

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

type writeRecorder struct {
	mu     sync.Mutex
	writes [][]byte
}

func (r *writeRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (r *writeRecorder) joined() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return bytes.Join(r.writes, nil)
}

func TestWriterFrameBoundsAndHumanReadability(t *testing.T) {
	rec := &writeRecorder{}
	w := NewWriter(rec)

	if _, err := w.WriteStream(StreamStdout, []byte("runs=1\n")); err != nil {
		t.Fatalf("WriteStream stdout: %v", err)
	}
	if _, err := w.WriteStream(StreamStderr, []byte("warn: low memory\n")); err != nil {
		t.Fatalf("WriteStream stderr: %v", err)
	}
	// Plain valid UTF-8 lines stay human-readable in kubectl logs.
	wire := string(rec.joined())
	if !strings.Contains(wire, "runs=1\n") {
		t.Fatalf("wire %q does not contain human-readable line %q", wire, "runs=1\n")
	}
	if !strings.Contains(wire, "\x1eK1 runs=1\n") {
		t.Fatalf("wire %q missing expected stdout frame", wire)
	}
	if !strings.Contains(wire, "\x1eK2 warn: low memory\n") {
		t.Fatalf("wire %q missing expected stderr frame", wire)
	}

	// Write a >16 KiB line and binary data; every individual Write must stay <= MaxFrameBytes (< PIPE_BUF).
	longLine := strings.Repeat("A", 16384) + "\n"
	if _, err := w.WriteStream(StreamStdout, []byte(longLine)); err != nil {
		t.Fatalf("WriteStream long line: %v", err)
	}
	bin := bytes.Repeat([]byte{0x00, 0x01, 0xff, '\r', '\n'}, 2000)
	if _, err := w.WriteStream(StreamStderr, bin); err != nil {
		t.Fatalf("WriteStream binary: %v", err)
	}
	for i, call := range rec.writes {
		if len(call) == 0 || len(call) > MaxFrameBytes {
			t.Fatalf("write %d len = %d; want 1..%d", i, len(call), MaxFrameBytes)
		}
		if call[len(call)-1] != '\n' {
			t.Fatalf("write %d does not end with newline: %q", i, call)
		}
	}
}

func TestRoundTripCases(t *testing.T) {
	type op struct {
		stream Stream
		data   []byte
	}
	cases := []struct {
		name       string
		ops        []op
		wantStdout []byte
		wantStderr []byte
	}{
		{
			name: "complete lines interleaved",
			ops: []op{
				{StreamStdout, []byte("out-1\nout-2\n")},
				{StreamStderr, []byte("err-1\n")},
				{StreamStdout, []byte("out-3\n")},
				{StreamStderr, []byte("err-2\nerr-3\n")},
			},
			wantStdout: []byte("out-1\nout-2\nout-3\n"),
			wantStderr: []byte("err-1\nerr-2\nerr-3\n"),
		},
		{
			name: "partial chunks without trailing newline",
			ops: []op{
				{StreamStdout, []byte("hello")},
				{StreamStderr, []byte("warn")},
				{StreamStdout, []byte(" world")},
				{StreamStderr, []byte("!")},
			},
			wantStdout: []byte("hello world"),
			wantStderr: []byte("warn!"),
		},
		{
			name: "consecutive partial chunks completed by newline",
			ops: []op{
				{StreamStdout, []byte("part1")},
				{StreamStdout, []byte("part2")},
				{StreamStdout, []byte("part3\n")},
				{StreamStdout, []byte("tail")},
			},
			wantStdout: []byte("part1part2part3\ntail"),
		},
		{
			name: "empty lines",
			ops: []op{
				{StreamStdout, []byte("\n\n")},
				{StreamStderr, []byte("\n")},
				{StreamStdout, []byte("x\n\ny\n")},
			},
			wantStdout: []byte("\n\nx\n\ny\n"),
			wantStderr: []byte("\n"),
		},
		{
			name: "binary NUL CR CRLF and invalid UTF-8",
			ops: []op{
				{StreamStdout, []byte("a\x00b\r\nc\xff\xfe")},
				{StreamStderr, []byte("\rprogress 50%\rprogress 100%\r\n\x00\x80")},
			},
			wantStdout: []byte("a\x00b\r\nc\xff\xfe"),
			wantStderr: []byte("\rprogress 50%\rprogress 100%\r\n\x00\x80"),
		},
		{
			name: "split multi-byte UTF-8 rune across WriteStream calls",
			ops: []op{
				// U+1F680 rocket: F0 9F 9A 80
				{StreamStdout, []byte{0xf0, 0x9f}},
				{StreamStderr, []byte{0xe2, 0x9c}},
				{StreamStdout, []byte{0x9a, 0x80, '\n'}},
				{StreamStderr, []byte{0x94, '\n'}},
			},
			wantStdout: []byte("🚀\n"),
			wantStderr: []byte("✔\n"),
		},
		{
			name: "long UTF-8 lines exceeding 4096 bytes with multi-byte runes on boundary",
			ops: []op{
				{StreamStdout, []byte(strings.Repeat("é", 3000) + "\n")},
				{StreamStderr, []byte(strings.Repeat("🚀", 2000))},
			},
			wantStdout: []byte(strings.Repeat("é", 3000) + "\n"),
			wantStderr: []byte(strings.Repeat("🚀", 2000)),
		},
		{
			name: "spoofed stderr frame inside stdout stays on stdout",
			ops: []op{
				{StreamStdout, []byte("\x1eK2 spoofed stderr line\n")},
				{StreamStdout, []byte("\x1eK2+spoofed partial")},
				{StreamStdout, []byte("\x1eK2bYWJj\n")},
				{StreamStderr, []byte("real stderr\n")},
			},
			wantStdout: []byte("\x1eK2 spoofed stderr line\n\x1eK2+spoofed partial\x1eK2bYWJj\n"),
			wantStderr: []byte("real stderr\n"),
		},
		{
			name: "binary line exceeding MaxBinaryPayload with trailing newline",
			ops: []op{
				{StreamStdout, append(bytes.Repeat([]byte{0x00, 0xff, '\r'}, 2500), '\n')},
			},
			wantStdout: append(bytes.Repeat([]byte{0x00, 0xff, '\r'}, 2500), '\n'),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wire bytes.Buffer
			w := NewWriter(&wire)
			for _, op := range tc.ops {
				n, err := w.WriteStream(op.stream, op.data)
				if err != nil || n != len(op.data) {
					t.Fatalf("WriteStream(%c, len=%d) = (%d, %v)", op.stream, len(op.data), n, err)
				}
			}
			// Full single-shot demux.
			res, err := Demux(bytes.NewReader(wire.Bytes()), 0, 0, 1<<20)
			if err != nil {
				t.Fatalf("Demux: %v", err)
			}
			if !bytes.Equal(res.Stdout, tc.wantStdout) {
				t.Fatalf("Stdout = %q (len %d); want %q (len %d)", res.Stdout, len(res.Stdout), tc.wantStdout, len(tc.wantStdout))
			}
			if !bytes.Equal(res.Stderr, tc.wantStderr) {
				t.Fatalf("Stderr = %q (len %d); want %q (len %d)", res.Stderr, len(res.Stderr), tc.wantStderr, len(tc.wantStderr))
			}
			if res.StdoutNext != int64(len(tc.wantStdout)) || res.StderrNext != int64(len(tc.wantStderr)) {
				t.Fatalf("Next = (%d, %d); want (%d, %d)", res.StdoutNext, res.StderrNext, len(tc.wantStdout), len(tc.wantStderr))
			}
			if res.StdoutTruncated || res.StderrTruncated {
				t.Fatalf("unexpected truncation: stdout=%v stderr=%v", res.StdoutTruncated, res.StderrTruncated)
			}
		})
	}
}

func TestDemuxPaginationLimits1To32(t *testing.T) {
	var wire bytes.Buffer
	w := NewWriter(&wire)
	wantStdout := []byte("alpha\nbeta\x00gamma\r\n" + "\x1eK2 spoof\n" + "tail-no-nl")
	wantStderr := []byte("err-one\nerr-two\x00\xff" + "err-tail")
	// Interleave small pieces to exercise multi-frame pagination.
	for i := 0; i < len(wantStdout) || i < len(wantStderr); i += 5 {
		if i < len(wantStdout) {
			end := min(i+5, len(wantStdout))
			_, _ = w.WriteStream(StreamStdout, wantStdout[i:end])
		}
		if i < len(wantStderr) {
			end := min(i+5, len(wantStderr))
			_, _ = w.WriteStream(StreamStderr, wantStderr[i:end])
		}
	}

	raw := wire.Bytes()
	for limit := 1; limit <= 32; limit++ {
		var gotOut, gotErr []byte
		var outOff, errOff int64
		for step := range 200 {
			res, err := Demux(bytes.NewReader(raw), outOff, errOff, limit)
			if err != nil {
				t.Fatalf("limit=%d step=%d Demux: %v", limit, step, err)
			}
			if len(res.Stdout) > limit || len(res.Stderr) > limit {
				t.Fatalf("limit=%d step=%d exceeded limit: out=%d err=%d", limit, step, len(res.Stdout), len(res.Stderr))
			}
			gotOut = append(gotOut, res.Stdout...)
			gotErr = append(gotErr, res.Stderr...)
			wantOutTrunc := res.StdoutNext < int64(len(wantStdout))
			wantErrTrunc := res.StderrNext < int64(len(wantStderr))
			if res.StdoutTruncated != wantOutTrunc || res.StderrTruncated != wantErrTrunc {
				t.Fatalf("limit=%d step=%d truncated=(%v,%v); want (%v,%v) at next=(%d,%d)",
					limit, step, res.StdoutTruncated, res.StderrTruncated, wantOutTrunc, wantErrTrunc, res.StdoutNext, res.StderrNext)
			}
			if res.StdoutNext == outOff && res.StderrNext == errOff {
				break
			}
			outOff, errOff = res.StdoutNext, res.StderrNext
		}
		if !bytes.Equal(gotOut, wantStdout) {
			t.Fatalf("limit=%d stdout = %q; want %q", limit, gotOut, wantStdout)
		}
		if !bytes.Equal(gotErr, wantStderr) {
			t.Fatalf("limit=%d stderr = %q; want %q", limit, gotErr, wantStderr)
		}
	}
}

func TestDemuxUnframedFallbackAndLongUnframedLines(t *testing.T) {
	t.Run("unframed lines with and without trailing newline", func(t *testing.T) {
		raw := "plain line 1\nplain line 2 without nl"
		res, err := Demux(strings.NewReader(raw), 0, 0, 1024)
		if err != nil {
			t.Fatalf("Demux: %v", err)
		}
		if string(res.Stdout) != raw || len(res.Stderr) != 0 {
			t.Fatalf("got stdout=%q stderr=%q; want stdout=%q stderr=''", res.Stdout, res.Stderr, raw)
		}
		if res.StdoutNext != int64(len(raw)) || res.StderrNext != 0 || res.StdoutTruncated || res.StderrTruncated {
			t.Fatalf("unexpected metadata: %+v", res)
		}
	})

	t.Run("malformed base64 frame falls back verbatim to stdout", func(t *testing.T) {
		raw := "\x1eK1b!!!not-base64!!!\n"
		res, err := Demux(strings.NewReader(raw), 0, 0, 1024)
		if err != nil {
			t.Fatalf("Demux: %v", err)
		}
		if string(res.Stdout) != raw || len(res.Stderr) != 0 {
			t.Fatalf("got stdout=%q stderr=%q; want verbatim stdout", res.Stdout, res.Stderr)
		}
	})

	t.Run("arbitrarily long unframed line (>128 KiB) streams safely with pagination", func(t *testing.T) {
		longLine := strings.Repeat("x", 128*1024) + "\n" + "after\n"
		res, err := Demux(strings.NewReader(longLine), 0, 0, 16)
		if err != nil {
			t.Fatalf("Demux: %v", err)
		}
		if string(res.Stdout) != strings.Repeat("x", 16) || !res.StdoutTruncated || res.StdoutNext != 16 {
			t.Fatalf("first page = %+v", res)
		}
		// Read across the end of the 128 KiB line into "after\n".
		tailOff := int64(128*1024 - 4)
		res2, err := Demux(strings.NewReader(longLine), tailOff, 0, 32)
		if err != nil {
			t.Fatalf("Demux tail: %v", err)
		}
		if want := "xxxx\nafter\n"; string(res2.Stdout) != want || res2.StdoutTruncated {
			t.Fatalf("tail = %q trunc=%v; want %q false", res2.Stdout, res2.StdoutTruncated, want)
		}
	})

	t.Run("long unframed line without trailing newline", func(t *testing.T) {
		longLine := strings.Repeat("y", 32*1024)
		res, err := Demux(strings.NewReader(longLine), 32*1024-5, 0, 16)
		if err != nil {
			t.Fatalf("Demux: %v", err)
		}
		if string(res.Stdout) != "yyyyy" || res.StdoutTruncated || res.StdoutNext != 32*1024 {
			t.Fatalf("got %+v", res)
		}
	})
}

type countingReader struct {
	r    io.Reader
	read int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += n
	return n, err
}

func TestDemuxEarlyStopWhenBothStreamsTruncated(t *testing.T) {
	var wire bytes.Buffer
	w := NewWriter(&wire)
	for range 200 {
		_, _ = w.WriteStream(StreamStdout, []byte(strings.Repeat("o", 100)+"\n"))
		_, _ = w.WriteStream(StreamStderr, []byte(strings.Repeat("e", 100)+"\n"))
	}
	cr := &countingReader{r: bytes.NewReader(wire.Bytes())}
	res, err := Demux(cr, 0, 0, 16)
	if err != nil {
		t.Fatalf("Demux: %v", err)
	}
	if !res.StdoutTruncated || !res.StderrTruncated || len(res.Stdout) != 16 || len(res.Stderr) != 16 {
		t.Fatalf("unexpected result: %+v", res)
	}
	// Must stop well before reading the entire ~42 KiB buffer.
	if cr.read >= wire.Len()/2 {
		t.Fatalf("Demux read %d of %d bytes; expected early stop", cr.read, wire.Len())
	}
}

func TestDemuxBeyondEndAndValidationAndConcurrentWriter(t *testing.T) {
	var wire bytes.Buffer
	w := NewWriter(&wire)
	_, _ = w.WriteStream(StreamStdout, []byte("abc\n"))
	_, _ = w.WriteStream(StreamStderr, []byte("xy\n"))

	beyond, err := Demux(bytes.NewReader(wire.Bytes()), 100, 50, 16)
	if err != nil {
		t.Fatalf("Demux beyond end: %v", err)
	}
	if len(beyond.Stdout) != 0 || beyond.StdoutNext != 100 || beyond.StdoutTruncated ||
		len(beyond.Stderr) != 0 || beyond.StderrNext != 50 || beyond.StderrTruncated {
		t.Fatalf("beyond = %+v; want empty no-progress at (100, 50)", beyond)
	}

	if _, err := Demux(nil, 0, 0, 10); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("nil reader err = %v; want ErrInvalidArguments", err)
	}
	if _, err := Demux(strings.NewReader(""), -1, 0, 10); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("negative stdout offset err = %v; want ErrInvalidArguments", err)
	}
	if _, err := Demux(strings.NewReader(""), 0, -1, 10); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("negative stderr offset err = %v; want ErrInvalidArguments", err)
	}
	if _, err := Demux(strings.NewReader(""), 0, 0, 0); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("zero limit err = %v; want ErrInvalidArguments", err)
	}
	if _, err := w.WriteStream(Stream('9'), []byte("x")); !errors.Is(err, ErrInvalidStream) {
		t.Fatalf("invalid stream err = %v; want ErrInvalidStream", err)
	}

	// Concurrent stdout + stderr writes through Writer must not race or corrupt frames.
	var concurrent bytes.Buffer
	cw := NewWriter(&concurrent)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			_, _ = cw.StreamWriter(StreamStdout).Write([]byte("stdout-line\n"))
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			_, _ = cw.StreamWriter(StreamStderr).Write([]byte("stderr-line\n"))
		}
	}()
	wg.Wait()

	res, err := Demux(bytes.NewReader(concurrent.Bytes()), 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("concurrent Demux: %v", err)
	}
	if want := strings.Repeat("stdout-line\n", 200); string(res.Stdout) != want {
		t.Fatalf("concurrent stdout len=%d; want %d", len(res.Stdout), len(want))
	}
	if want := strings.Repeat("stderr-line\n", 200); string(res.Stderr) != want {
		t.Fatalf("concurrent stderr len=%d; want %d", len(res.Stderr), len(want))
	}
}
