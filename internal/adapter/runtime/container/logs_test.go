package container

import (
	"slices"
	"testing"

	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

func TestValidateLogsRequest(t *testing.T) {
	valid := reconcile.LogsRequest{
		Identity:    validIdentity(),
		AfterOffset: 0,
		Limit:       1024,
	}
	if err := ValidateLogsRequest(valid); err != nil {
		t.Fatalf("ValidateLogsRequest(valid) = %v; want nil", err)
	}
	valid.Limit = MaxLogBytes
	if err := ValidateLogsRequest(valid); err != nil {
		t.Fatalf("ValidateLogsRequest(MaxLogBytes) = %v; want nil", err)
	}

	cases := []struct {
		name   string
		mutate func(*reconcile.LogsRequest)
	}{
		{"invalid identity", func(r *reconcile.LogsRequest) { r.Identity.WorkloadID = "" }},
		{"zero limit", func(r *reconcile.LogsRequest) { r.Limit = 0 }},
		{"negative limit", func(r *reconcile.LogsRequest) { r.Limit = -1 }},
		{"oversized limit", func(r *reconcile.LogsRequest) { r.Limit = MaxLogBytes + 1 }},
		{"negative offset", func(r *reconcile.LogsRequest) { r.AfterOffset = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mutate(&req)
			assertFaultClass(t, ValidateLogsRequest(req), fault.InvalidInput)
		})
	}
}

func TestSortRedactionValues(t *testing.T) {
	got := SortRedactionValues([]string{"", "bb", "aaa", "aa", "bbb", ""})
	want := []string{"aaa", "bbb", "aa", "bb"}
	if !slices.Equal(got, want) {
		t.Fatalf("SortRedactionValues = %v; want %v", got, want)
	}
}

func TestRedactLogPage(t *testing.T) {
	values := SortRedactionValues([]string{"secret-long", "secret"})

	// Longest match wins without splitting.
	consumed, redacted := RedactLogPage([]byte("pre secret-long post"), values, 64, false)
	if string(consumed) != "pre secret-long post" || string(redacted) != "pre [REDACTED] post" {
		t.Fatalf("got (%q, %q)", consumed, redacted)
	}

	// Holds trailing partial prefix when mayGrow is true.
	consumed, redacted = RedactLogPage([]byte("hello sec"), values, 64, true)
	if string(consumed) != "hello " || string(redacted) != "hello " {
		t.Fatalf("mayGrow hold = (%q, %q); want (%q, %q)", consumed, redacted, "hello ", "hello ")
	}

	// Flushes trailing partial prefix at EOF (mayGrow=false).
	consumed, redacted = RedactLogPage([]byte("hello sec"), values, 64, false)
	if string(consumed) != "hello sec" || string(redacted) != "hello sec" {
		t.Fatalf("eof flush = (%q, %q); want (%q, %q)", consumed, redacted, "hello sec", "hello sec")
	}

	// Held at position 0 emits nothing and advances nothing.
	consumed, redacted = RedactLogPage([]byte("sec"), values, 64, true)
	if len(consumed) != 0 || len(redacted) != 0 {
		t.Fatalf("held at 0 = (%q, %q); want empty", consumed, redacted)
	}

	// Marker expansion bounded by limit does not drop raw bytes.
	shortSecret := SortRedactionValues([]string{"ab"})
	consumed, redacted = RedactLogPage([]byte("abXY"), shortSecret, len(Redacted), false)
	if string(consumed) != "ab" || string(redacted) != Redacted {
		t.Fatalf("marker bounded = (%q, %q); want (%q, %q)", consumed, redacted, "ab", Redacted)
	}

	// Single complete marker larger than limit is clipped and consumed.
	consumed, redacted = RedactLogPage([]byte("abXY"), shortSecret, 4, false)
	if string(consumed) != "ab" || string(redacted) != Redacted[:4] {
		t.Fatalf("clipped marker = (%q, %q); want (%q, %q)", consumed, redacted, "ab", Redacted[:4])
	}
}

func TestLogCursorEncodingAndOverflow(t *testing.T) {
	cursor := EncodeLogCursor(12345, 67890)
	out, errPos := DecodeLogCursor(cursor)
	if out != 12345 || errPos != 67890 {
		t.Fatalf("DecodeLogCursor = (%d, %d); want (12345, 67890)", out, errPos)
	}

	maxCursor := EncodeLogCursor(MaxStdoutLogCursorPosition, MaxStderrLogCursorPosition)
	if maxCursor < 0 {
		t.Fatalf("max cursor became negative: %d", maxCursor)
	}
	out, errPos = DecodeLogCursor(maxCursor)
	if out != MaxStdoutLogCursorPosition || errPos != MaxStderrLogCursorPosition {
		t.Fatalf("max roundtrip = (%d, %d)", out, errPos)
	}

	if pos, err := NextStreamLogPosition(reconcile.LogStdout, MaxStdoutLogCursorPosition, 0); err != nil || pos != MaxStdoutLogCursorPosition {
		t.Fatalf("stdout boundary = (%d, %v)", pos, err)
	}
	_, err := NextStreamLogPosition(reconcile.LogStdout, MaxStdoutLogCursorPosition, 1)
	assertFaultClass(t, err, fault.InvalidInput)

	if pos, err := NextStreamLogPosition(reconcile.LogStderr, MaxStderrLogCursorPosition, 0); err != nil || pos != MaxStderrLogCursorPosition {
		t.Fatalf("stderr boundary = (%d, %v)", pos, err)
	}
	_, err = NextStreamLogPosition(reconcile.LogStderr, MaxStderrLogCursorPosition, 1)
	assertFaultClass(t, err, fault.InvalidInput)

	_, err = NextStreamLogPosition(reconcile.LogStdout, -1, 1)
	assertFaultClass(t, err, fault.InvalidInput)
	_, err = NextStreamLogPosition(reconcile.LogStdout, 0, -1)
	assertFaultClass(t, err, fault.InvalidInput)
}

func TestBuildLogChunk(t *testing.T) {
	values := SortRedactionValues([]string{"s3cr3t"})

	// Normal page with redaction and expansion causing partial consumption sets Truncated=true.
	chunk, hasChunk, nextPos, err := BuildLogChunk(LogPageInput{
		Stream:        reconcile.LogStdout,
		Offset:        10,
		Sequence:      1,
		Raw:           []byte("s3cr3t-tail"),
		PageTruncated: false,
		Values:        values,
		Limit:         len(Redacted),
		MayGrow:       false,
	})
	if err != nil {
		t.Fatalf("BuildLogChunk = %v; want nil", err)
	}
	if !hasChunk {
		t.Fatal("expected hasChunk=true")
	}
	if nextPos != 16 {
		t.Fatalf("nextPos = %d; want 16", nextPos)
	}
	if chunk.Stream != reconcile.LogStdout || chunk.Offset != 10 || chunk.Sequence != 1 ||
		string(chunk.Data) != Redacted || !chunk.Truncated {
		t.Fatalf("unexpected chunk: %+v", chunk)
	}

	// Held partial prefix produces hasChunk=false and keeps offset unchanged.
	_, hasChunk, nextPos, err = BuildLogChunk(LogPageInput{
		Stream:        reconcile.LogStderr,
		Offset:        5,
		Sequence:      2,
		Raw:           []byte("s3c"),
		PageTruncated: true,
		Values:        values,
		Limit:         64,
		MayGrow:       true,
	})
	if err != nil {
		t.Fatalf("BuildLogChunk(held) = %v; want nil", err)
	}
	if hasChunk || nextPos != 5 {
		t.Fatalf("held chunk = (hasChunk=%v, nextPos=%d); want (false, 5)", hasChunk, nextPos)
	}

	// Overflow surfaces fault.InvalidInput.
	_, _, _, err = BuildLogChunk(LogPageInput{
		Stream:   reconcile.LogStdout,
		Offset:   MaxStdoutLogCursorPosition,
		Sequence: 1,
		Raw:      []byte("x"),
		Limit:    64,
	})
	assertFaultClass(t, err, fault.InvalidInput)

	if ErrRedactionSourceUnavailable == nil {
		t.Fatal("ErrRedactionSourceUnavailable must be non-nil")
	}

	// Non-positive limits never panic or consume raw bytes.
	for _, limit := range []int{0, -1, -100} {
		consumed, data := RedactLogPage([]byte("s3cr3t"), values, limit, false)
		if consumed != nil || data != nil {
			t.Fatalf("RedactLogPage(limit=%d) = (%q, %q); want (nil, nil)", limit, consumed, data)
		}
	}
}
