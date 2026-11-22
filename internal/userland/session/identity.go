package session

import "fmt"

// SessionID is one bounded durable session identity.
//
// A session identity names the session stream whose revisions a checkpoint
// belongs to. It is never a process ID, a scratch path or a worktree path.
type SessionID string

// maxSessionIDBytes bounds one session identity.
const maxSessionIDBytes = 128

// ParseSessionID validates and converts a session identity.
func ParseSessionID(value string) (SessionID, error) {
	if !validSessionID(value) {
		return "", fmt.Errorf("invalid session id %q", value)
	}
	return SessionID(value), nil
}

// validSessionID reports whether value is a bounded ASCII identity. Dots are
// refused so an identity can never be confused with a path component.
func validSessionID(value string) bool {
	return validASCIIIdentifier(value, maxSessionIDBytes)
}
