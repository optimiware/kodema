package servicekit

import (
	"time"
	"unicode/utf8"
)

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
