package container

import (
	"errors"
	"sort"

	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

// ErrRedactionSourceUnavailable reports that the configured credential or
// secret values needed to redact log output could not be loaded, so no output
// may be emitted.
var ErrRedactionSourceUnavailable = errors.New("log redaction source unavailable")

// ValidateLogsRequest refuses an invalid identity, out-of-range limit, or
// negative offset before any runtime boundary call.
func ValidateLogsRequest(request reconcile.LogsRequest) error {
	if err := ValidateIdentity(request.Identity); err != nil {
		return err
	}
	if request.Limit <= 0 || request.Limit > MaxLogBytes {
		return fault.New(fault.InvalidInput, "log read limit is outside the bounded range")
	}
	if request.AfterOffset < 0 {
		return fault.New(fault.InvalidInput, "log read offset is negative")
	}
	return nil
}

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

type logSegment struct {
	start  int
	end    int
	marker bool
	data   []byte
}

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
	if limit <= 0 {
		return nil, nil
	}
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
			return nil, nil
		}
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
func NextStreamLogPosition(stream reconcile.LogStream, offset int64, consumed int) (int64, error) {
	bound := MaxStdoutLogCursorPosition
	if stream == reconcile.LogStderr {
		bound = MaxStderrLogCursorPosition
	}
	if offset < 0 || offset > bound || consumed < 0 || int64(consumed) > bound-offset {
		return 0, fault.New(fault.InvalidInput,
			"log position exceeds the continuation-token range")
	}
	return offset + int64(consumed), nil
}

// LogPageInput holds one raw runtime log page and the bounded redaction
// parameters needed to produce an ordered, redacted chunk and next cursor.
type LogPageInput struct {
	Stream        reconcile.LogStream
	Offset        int64
	Sequence      int64
	Raw           []byte
	PageTruncated bool
	Values        []string
	Limit         int
	MayGrow       bool
}

// BuildLogChunk redacts one raw stream page, advances the stream cursor over
// only the consumed raw bytes, and constructs the resulting LogChunk when any
// raw bytes were consumed.
func BuildLogChunk(in LogPageInput) (chunk reconcile.LogChunk, hasChunk bool, nextPos int64, err error) {
	consumed, data := RedactLogPage(in.Raw, in.Values, in.Limit, in.MayGrow)
	nextPos, err = NextStreamLogPosition(in.Stream, in.Offset, len(consumed))
	if err != nil {
		return reconcile.LogChunk{}, false, 0, err
	}
	if len(consumed) == 0 {
		return reconcile.LogChunk{}, false, nextPos, nil
	}
	return reconcile.LogChunk{
		Stream:    in.Stream,
		Offset:    in.Offset,
		Sequence:  in.Sequence,
		Data:      data,
		Truncated: in.PageTruncated || len(consumed) < len(in.Raw),
	}, true, nextPos, nil
}
