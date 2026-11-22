// Package logframe implements a compact, line-oriented, binary-safe wire
// framing protocol for multiplexing stdout and stderr over Kubernetes' single
// combined container log stream.
//
// Every emitted frame is strictly smaller than PIPE_BUF (4096 bytes) so each
// Write to container stdout is atomic and never split by CRI/kubelet:
//
//   - "\x1eK1 <line>\n"   -> stdout complete line (decodes to "<line>\n")
//   - "\x1eK2 <line>\n"   -> stderr complete line (decodes to "<line>\n")
//   - "\x1eK1+<chunk>\n"  -> stdout partial chunk without trailing "\n"
//   - "\x1eK2+<chunk>\n"  -> stderr partial chunk without trailing "\n"
//   - "\x1eK1b<base64>\n" -> stdout binary-safe chunk (used when bytes contain
//     NUL, CR, LF, or invalid UTF-8)
//   - "\x1eK2b<base64>\n" -> stderr binary-safe chunk
//
// Any input line that does not begin with a valid frame header is decoded
// verbatim as stdout, preserving 100% backward compatibility with pre-upgrade
// pods and non-toolbox workloads.
package logframe

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"sync"
	"unicode/utf8"
)

// Stream identifies one standard output stream in the wire protocol.
type Stream byte

const (
	// StreamStdout identifies standard output (fd 1).
	StreamStdout Stream = '1'
	// StreamStderr identifies standard error (fd 2).
	StreamStderr Stream = '2'
)

const (
	// PrefixByte is ASCII Record Separator (0x1e), chosen so ordinary log
	// output stays readable while frame headers remain unambiguous.
	PrefixByte byte = 0x1e
	// HeaderLen is the fixed four-byte frame prefix: "\x1eK<stream><kind>".
	HeaderLen = 4
	// MaxFrameBytes bounds every encoded frame below Linux PIPE_BUF (4096) so
	// each Write syscall is atomic and CRI never splits a frame across lines.
	MaxFrameBytes = 4094
	// MaxRawPayload is the maximum raw UTF-8 payload in one frame (excluding
	// the 4-byte header and trailing '\n').
	MaxRawPayload = MaxFrameBytes - HeaderLen - 1
	// MaxBinaryPayload is the maximum raw byte count whose standard base64
	// encoding fits within MaxRawPayload.
	MaxBinaryPayload = (MaxRawPayload / 4) * 3

	// demuxBufferSize exceeds MaxFrameBytes so any valid frame always completes
	// in a single bufio.Reader.ReadSlice('\n') call; any line returning
	// bufio.ErrBufferFull is guaranteed to be an unframed line.
	demuxBufferSize = 8192
)

var (
	// ErrInvalidStream reports an unknown stream identifier.
	ErrInvalidStream = errors.New("logframe: invalid stream")
	// ErrInvalidArguments reports negative offsets or a non-positive limit.
	ErrInvalidArguments = errors.New("logframe: invalid demux arguments")
)

// Writer serializes framed stdout and stderr records onto one underlying writer
// (typically container stdout). Each frame is emitted in a single Write call
// under a mutex so concurrent stream writers never interleave bytes and live
// output has zero buffering latency.
type Writer struct {
	mu  sync.Mutex
	w   io.Writer
	buf []byte
}

// NewWriter constructs a frame Writer targeting w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:   w,
		buf: make([]byte, 0, MaxFrameBytes),
	}
}

// StreamWriter returns an io.Writer that writes all bytes to stream.
func (w *Writer) StreamWriter(stream Stream) io.Writer {
	return streamWriter{w: w, stream: stream}
}

type streamWriter struct {
	w      *Writer
	stream Stream
}

func (s streamWriter) Write(p []byte) (int, error) {
	return s.w.WriteStream(s.stream, p)
}

// WriteStream frames p for stream and writes every frame immediately to the
// underlying writer. It handles arbitrary chunk sizes, partial lines, NUL/CR
// bytes, and split multi-byte UTF-8 sequences losslessly.
func (w *Writer) WriteStream(stream Stream, p []byte) (int, error) {
	if stream != StreamStdout && stream != StreamStderr {
		return 0, ErrInvalidStream
	}
	if len(p) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	rest := p
	for len(rest) > 0 {
		nl := bytes.IndexByte(rest, '\n')
		hasNewline := nl >= 0
		var seg []byte
		if hasNewline {
			seg = rest[:nl]
			rest = rest[nl+1:]
		} else {
			seg = rest
			rest = nil
		}

		if needsBinary(seg) {
			if hasNewline {
				// Include the trailing '\n' in the binary stream so exact
				// bytes (including CRLF or NUL before LF) round-trip cleanly.
				if err := w.writeBinaryWithNewlineLocked(stream, seg); err != nil {
					return 0, err
				}
			} else {
				if err := w.writeBinaryLocked(stream, seg); err != nil {
					return 0, err
				}
			}
			continue
		}

		if err := w.writeTextLocked(stream, seg, hasNewline); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// needsBinary reports whether seg contains any byte that cannot be represented
// directly in a text frame (NUL, CR, LF, or invalid UTF-8).
func needsBinary(seg []byte) bool {
	if bytes.IndexByte(seg, 0) >= 0 || bytes.IndexByte(seg, '\r') >= 0 || bytes.IndexByte(seg, '\n') >= 0 {
		return true
	}
	return !utf8.Valid(seg)
}

func (w *Writer) writeTextLocked(stream Stream, seg []byte, hasNewline bool) error {
	if len(seg) == 0 {
		if !hasNewline {
			return nil
		}
		return w.emitFrameLocked(stream, ' ', nil)
	}
	for len(seg) > MaxRawPayload {
		cut := utf8SplitPoint(seg, MaxRawPayload)
		if err := w.emitFrameLocked(stream, '+', seg[:cut]); err != nil {
			return err
		}
		seg = seg[cut:]
	}
	kind := byte('+')
	if hasNewline {
		kind = ' '
	}
	return w.emitFrameLocked(stream, kind, seg)
}

func (w *Writer) writeBinaryLocked(stream Stream, data []byte) error {
	for len(data) > 0 {
		chunkLen := min(len(data), MaxBinaryPayload)
		if err := w.emitBinaryFrameLocked(stream, data[:chunkLen]); err != nil {
			return err
		}
		data = data[chunkLen:]
	}
	return nil
}

func (w *Writer) writeBinaryWithNewlineLocked(stream Stream, seg []byte) error {
	for len(seg) >= MaxBinaryPayload {
		if err := w.emitBinaryFrameLocked(stream, seg[:MaxBinaryPayload]); err != nil {
			return err
		}
		seg = seg[MaxBinaryPayload:]
	}
	// Combine remaining tail (< MaxBinaryPayload) with '\n' into one final
	// binary frame (at most MaxBinaryPayload bytes).
	combined := make([]byte, len(seg)+1)
	copy(combined, seg)
	combined[len(seg)] = '\n'
	return w.emitBinaryFrameLocked(stream, combined)
}

func (w *Writer) emitFrameLocked(stream Stream, kind byte, payload []byte) error {
	w.buf = w.buf[:0]
	w.buf = append(w.buf, PrefixByte, 'K', byte(stream), kind)
	w.buf = append(w.buf, payload...)
	w.buf = append(w.buf, '\n')
	_, err := w.w.Write(w.buf)
	return err
}

func (w *Writer) emitBinaryFrameLocked(stream Stream, raw []byte) error {
	encodedLen := base64.StdEncoding.EncodedLen(len(raw))
	total := HeaderLen + encodedLen + 1
	if cap(w.buf) < total {
		w.buf = make([]byte, total)
	} else {
		w.buf = w.buf[:total]
	}
	w.buf[0] = PrefixByte
	w.buf[1] = 'K'
	w.buf[2] = byte(stream)
	w.buf[3] = 'b'
	base64.StdEncoding.Encode(w.buf[HeaderLen:HeaderLen+encodedLen], raw)
	w.buf[total-1] = '\n'
	_, err := w.w.Write(w.buf)
	return err
}

// utf8SplitPoint picks the largest cut <= maxBytes that does not split a
// multi-byte UTF-8 rune. Caller guarantees seg is valid UTF-8 and len(seg) > maxBytes.
func utf8SplitPoint(seg []byte, maxBytes int) int {
	cut := maxBytes
	for cut > 0 && (seg[cut]&0xc0) == 0x80 {
		cut--
	}
	if cut == 0 {
		return maxBytes
	}
	return cut
}

// DemuxResult holds one bounded, single-pass demultiplexed page for both
// stdout and stderr.
type DemuxResult struct {
	Stdout          []byte
	StdoutNext      int64
	StdoutTruncated bool
	Stderr          []byte
	StderrNext      int64
	StderrTruncated bool
}

type streamCollector struct {
	offset    int64
	seen      int64
	limit     int
	buf       []byte
	truncated bool
}

func (c *streamCollector) add(data []byte) {
	if len(data) == 0 || c.truncated {
		return
	}
	end := c.seen + int64(len(data))
	if end <= c.offset {
		c.seen = end
		return
	}
	if c.seen < c.offset {
		skip := c.offset - c.seen
		data = data[skip:]
		c.seen = c.offset
	}
	remaining := c.limit - len(c.buf)
	if remaining <= 0 {
		c.truncated = true
		return
	}
	if len(data) > remaining {
		c.buf = append(c.buf, data[:remaining]...)
		c.seen += int64(remaining)
		c.truncated = true
		return
	}
	c.buf = append(c.buf, data...)
	c.seen += int64(len(data))
}

func (c *streamCollector) nextOffset() int64 {
	return c.offset + int64(len(c.buf))
}

// Demux reads r in a single streaming pass with O(limit) memory, demultiplexing
// framed records into stdout and stderr starting at their respective byte
// offsets and collecting at most limit bytes per stream.
//
// Any line that does not begin with a valid frame header is attributed verbatim
// to stdout, preserving backward compatibility with unframed container logs.
// Once both streams have observed more than limit bytes past their offsets,
// Demux stops reading early.
func Demux(r io.Reader, stdoutOffset, stderrOffset int64, limit int) (DemuxResult, error) {
	if r == nil || stdoutOffset < 0 || stderrOffset < 0 || limit <= 0 {
		return DemuxResult{}, ErrInvalidArguments
	}
	br := bufio.NewReaderSize(r, demuxBufferSize)
	stdout := streamCollector{offset: stdoutOffset, limit: limit}
	stderr := streamCollector{offset: stderrOffset, limit: limit}
	var decodeBuf []byte

	for !(stdout.truncated && stderr.truncated) {
		fragment, err := br.ReadSlice('\n')
		switch {
		case err == nil:
			// Complete line ending with '\n' and fitting in demuxBufferSize.
			decodeLine(fragment, &stdout, &stderr, &decodeBuf)
		case errors.Is(err, io.EOF):
			if len(fragment) > 0 {
				// Trailing bytes without '\n' are never a complete frame;
				// emit verbatim as unframed stdout without inventing a newline.
				stdout.add(fragment)
			}
			return buildDemuxResult(&stdout, &stderr), nil
		case errors.Is(err, bufio.ErrBufferFull):
			// Every valid frame is <= MaxFrameBytes (< demuxBufferSize), so a
			// line overflowing the buffer cannot be framed. Stream it straight
			// to stdout in bounded chunks without unbounded buffering.
			stdout.add(fragment)
			for errors.Is(err, bufio.ErrBufferFull) {
				if stdout.truncated && stderr.truncated {
					return buildDemuxResult(&stdout, &stderr), nil
				}
				fragment, err = br.ReadSlice('\n')
				if len(fragment) > 0 {
					stdout.add(fragment)
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					return buildDemuxResult(&stdout, &stderr), nil
				}
				return DemuxResult{}, err
			}
		default:
			return DemuxResult{}, err
		}
	}
	return buildDemuxResult(&stdout, &stderr), nil
}

func decodeLine(line []byte, stdout, stderr *streamCollector, decodeBuf *[]byte) {
	if len(line) >= HeaderLen+1 && line[0] == PrefixByte && line[1] == 'K' {
		var target *streamCollector
		switch Stream(line[2]) {
		case StreamStdout:
			target = stdout
		case StreamStderr:
			target = stderr
		}
		if target != nil {
			switch line[3] {
			case ' ':
				// Complete line: payload plus the existing trailing '\n'.
				target.add(line[HeaderLen:])
				return
			case '+':
				// Partial chunk: payload without the frame's trailing '\n'.
				target.add(line[HeaderLen : len(line)-1])
				return
			case 'b':
				payload := line[HeaderLen : len(line)-1]
				maxDec := base64.StdEncoding.DecodedLen(len(payload))
				if cap(*decodeBuf) < maxDec {
					*decodeBuf = make([]byte, maxDec)
				} else {
					*decodeBuf = (*decodeBuf)[:maxDec]
				}
				n, err := base64.StdEncoding.Decode(*decodeBuf, payload)
				if err == nil {
					target.add((*decodeBuf)[:n])
					return
				}
				// Malformed base64 falls through to unframed stdout verbatim.
			}
		}
	}
	stdout.add(line)
}

func buildDemuxResult(stdout, stderr *streamCollector) DemuxResult {
	return DemuxResult{
		Stdout:          stdout.buf,
		StdoutNext:      stdout.nextOffset(),
		StdoutTruncated: stdout.truncated,
		Stderr:          stderr.buf,
		StderrNext:      stderr.nextOffset(),
		StderrTruncated: stderr.truncated,
	}
}
