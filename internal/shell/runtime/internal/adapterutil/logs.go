package adapterutil

import (
	"sort"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/kernel/engine"
)

// SortRedactionValues returns all non-empty values ordered longest first (with
// lexicographic tie-break) so a shorter value cannot partially obscure a longer
// one.
func SortRedactionValues(values []string) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		filtered = append(filtered, value)
	}
	sort.Slice(filtered, func(left, right int) bool {
		if len(filtered[left]) != len(filtered[right]) {
			return len(filtered[left]) > len(filtered[right])
		}
		return filtered[left] < filtered[right]
	})
	return filtered
}

// logSegment is one decoded piece of a log page: a raw byte range and the
// bytes it contributes after redaction (a raw copy or the fixed marker).
type logSegment struct {
	start  int
	end    int
	marker bool
	data   []byte
}

// scanLogSegments splits a page into raw runs and configured-value matches,
// left to right and longest value first (SortRedactionValues orders them that
// way). A value is consumed atomically, so no page boundary can be placed
// inside one.
func scanLogSegments(data []byte, values []string) []logSegment {
	segments := make([]logSegment, 0, len(data))
	for index := 0; index < len(data); {
		matched := false
		for _, value := range values {
			if value == "" || len(value) > len(data)-index {
				continue
			}
			if string(data[index:index+len(value)]) == value {
				segments = append(segments, logSegment{
					start:  index,
					end:    index + len(value),
					marker: true,
					data:   []byte(Redacted),
				})
				index += len(value)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		segments = append(segments, logSegment{
			start: index,
			end:   index + 1,
			data:  []byte{data[index]},
		})
		index++
	}
	return segments
}

// couldExtend reports whether tail is a proper prefix of a configured value, so
// a longer match could still start there once the log grows past this page. The
// whole visible tail must match, not just the bytes already recognised: every
// byte the page does contain is fixed in the log.
func couldExtend(tail []byte, values []string) bool {
	for _, value := range values {
		if len(value) <= len(tail) {
			continue
		}
		if hasStringPrefix(value, tail) {
			return true
		}
	}
	return false
}

// hasStringPrefix reports whether value starts with prefix without allocating.
func hasStringPrefix(value string, prefix []byte) bool {
	if len(prefix) > len(value) {
		return false
	}
	for index := range prefix {
		if value[index] != prefix[index] {
			return false
		}
	}
	return true
}

// RedactLogPage redacts one bounded page without splitting a configured value
// across the page boundary or dropping raw bytes when marker expansion exceeds
// limit. It returns the raw prefix the cursor may advance over and the redacted
// bytes to emit. mayGrow says the page is a strict prefix of the log: more
// bytes exist past it, so a match near its end may still be extended by a
// longer configured value on a later page.
func RedactLogPage(raw []byte, values []string, limit int, mayGrow bool) ([]byte, []byte) {
	segments := scanLogSegments(raw, values)
	end := len(segments)
	hold := end
	if mayGrow {
		for index, segment := range segments {
			if couldExtend(raw[segment.start:], values) {
				end = index
				hold = index
				break
			}
		}
	}

	size := 0
	for _, segment := range segments[:end] {
		size += len(segment.data)
	}
	for end > 0 && size > limit {
		end--
		size -= len(segments[end].data)
	}
	if end == 0 {
		if len(segments) == 0 || hold == 0 {
			// Nothing before the held position resolved, so no raw value bytes
			// must be emitted and no bytes may be skipped.
			return nil, nil
		}
		// Only reachable when the first segment is a marker that alone exceeds
		// the bound. A held first segment returned above, so this marker is
		// complete: its bytes may be clipped and the value consumed, while any
		// held tail after it is re-read from the cursor.
		first := segments[0]
		data := first.data
		if len(data) > limit {
			data = data[:limit]
		}
		return raw[:first.end], data
	}

	consumed := raw[:segments[end-1].end]
	data := make([]byte, 0, size)
	for _, segment := range segments[:end] {
		data = append(data, segment.data...)
	}
	return consumed, data
}

// logCursorStreamBits splits the single log continuation token into one
// position per stream: the high 32 bits carry stdout (or combined log) and the
// low 32 bits carry stderr.
const logCursorStreamBits = 32

const logCursorStreamMask int64 = (1 << logCursorStreamBits) - 1

const (
	// MaxStdoutLogCursorPosition bounds the signed high 32-bit stdout half so
	// EncodeLogCursor never sets the sign bit and returns a negative cursor.
	MaxStdoutLogCursorPosition int64 = (1 << 31) - 1
	// MaxStderrLogCursorPosition bounds the unsigned low 32-bit stderr half so
	// stderr never overflows into the stdout bits.
	MaxStderrLogCursorPosition int64 = (1 << 32) - 1
)

// DecodeLogCursor returns the per-stream positions encoded in one token.
func DecodeLogCursor(cursor int64) (stdout, stderr int64) {
	return cursor >> logCursorStreamBits, cursor & logCursorStreamMask
}

// EncodeLogCursor packs per-stream positions into one continuation token.
func EncodeLogCursor(stdout, stderr int64) int64 {
	return stdout<<logCursorStreamBits | stderr
}

// NextStreamLogPosition advances one stream's cursor position while refusing
// any position that cannot be represented losslessly in the continuation token.
func NextStreamLogPosition(stream engine.LogStream, offset int64, consumed int) (int64, error) {
	bound := MaxStdoutLogCursorPosition
	if stream == engine.LogStderr {
		bound = MaxStderrLogCursorPosition
	}
	if offset < 0 || offset > bound || consumed < 0 || int64(consumed) > bound-offset {
		return 0, fault.New(fault.InvalidInput,
			"log position exceeds the continuation-token range")
	}
	return offset + int64(consumed), nil
}
