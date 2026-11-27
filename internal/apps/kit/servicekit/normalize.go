package servicekit

import (
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const maxDiagnosticNoteBytes = 512

var (
	reKodemaToken = regexp.MustCompile(`\b(?:kodex|kodsvc)\.[A-Za-z0-9._-]+\b`)
	reAuthHeader  = regexp.MustCompile(`(?i)(Authorization\s*:\s*(?:Basic|Bearer)\s+)\S+`)
	reURLUserinfo = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)([^\s/@]+)@`)
	reJWT         = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`)
	reSecretKV    = regexp.MustCompile(`(?i)\b(token|password|passwd|secret|api[_-]?key|credential)(=)([^\s&;,'"]+)`)
	reInvalidUTF8 = func(s string) string { return strings.ToValidUTF8(s, "") }
)

// SanitizeDiagnosticNote produces a single-line, UTF-8-valid, credential-free,
// bounded diagnostic note safe to attach to a request `fail` decision.
func SanitizeDiagnosticNote(raw string) string {
	cleaned := reInvalidUTF8(raw)
	cleaned = reAuthHeader.ReplaceAllString(cleaned, "${1}[redacted]")
	cleaned = reURLUserinfo.ReplaceAllString(cleaned, "${1}[redacted]@")
	cleaned = reKodemaToken.ReplaceAllString(cleaned, "[redacted]")
	cleaned = reJWT.ReplaceAllString(cleaned, "[redacted]")
	cleaned = reSecretKV.ReplaceAllString(cleaned, "${1}${2}[redacted]")

	var builder strings.Builder
	builder.Grow(len(cleaned))
	for i := 0; i < len(cleaned); i++ {
		ch := cleaned[i]
		if ch == '\r' || ch == '\n' || ch == '\t' {
			builder.WriteByte(' ')
			continue
		}
		if ch < 0x20 || ch == 0x7f {
			continue
		}
		builder.WriteByte(ch)
	}
	collapsed := strings.Join(strings.Fields(builder.String()), " ")
	return TruncateString(collapsed, maxDiagnosticNoteBytes)
}

// NormalizedTime removes the monotonic clock reading and fixes the location to
// UTC. A durable record crosses a JSON boundary that cannot carry either, so
// every in-memory record uses this canonical form and value comparisons stay
// stable across that round trip.
func NormalizedTime(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value.UTC().Round(0)
}

// TruncateString bounds s to at most maxBytes without splitting a multi-byte
// UTF-8 sequence.
func TruncateString(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	limit := maxBytes
	minLimit := max(0, maxBytes-(utf8.UTFMax-1))
	for limit > minLimit && !utf8.RuneStart(s[limit]) {
		limit--
	}
	if !utf8.RuneStart(s[limit]) {
		return s[:maxBytes]
	}
	return s[:limit]
}
