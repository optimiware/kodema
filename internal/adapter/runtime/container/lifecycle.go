package container

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/fault"
)

// Clock is the injected time source shared by runtime adapters. Production
// uses RealClock; tests inject a deterministic clock that never sleeps in
// wall-clock time.
type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

// RealClock is the production clock shared by runtime adapters.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep waits for duration or until ctx is cancelled.
func (RealClock) Sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type keyedLockEntry struct {
	mu   sync.Mutex
	refs int
}

// KeyedLock provides reference-counted per-key mutual exclusion. The zero value
// is ready for immediate concurrent use, and map entries are deleted once the
// last waiter releases the key.
type KeyedLock[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*keyedLockEntry
}

// Lock acquires the mutex for key and returns an idempotent release function.
func (l *KeyedLock[K]) Lock(key K) func() {
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[K]*keyedLockEntry)
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &keyedLockEntry{}
		l.entries[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.Unlock()
			l.mu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(l.entries, key)
			}
			l.mu.Unlock()
		})
	}
}

// CleanupPolicy bounds retry attempts per cleanup operation.
type CleanupPolicy struct {
	// Attempts is the total attempts per resource per Remove call; at least 1.
	Attempts int
	// Backoff is the delay between attempts, applied through Clock.Sleep.
	Backoff time.Duration
}

// DefaultCleanupPolicy returns the default bounded cleanup retry policy.
func DefaultCleanupPolicy() CleanupPolicy {
	return CleanupPolicy{
		Attempts: 3,
		Backoff:  50 * time.Millisecond,
	}
}

// Validate checks that Attempts is at least 1 and Backoff is non-negative.
func (p CleanupPolicy) Validate() error {
	if p.Attempts < 1 {
		return fault.New(fault.InvalidInput, "cleanup policy requires at least one attempt")
	}
	if p.Backoff < 0 {
		return fault.New(fault.InvalidInput, "cleanup policy backoff is negative")
	}
	return nil
}

// RetryWithBackoff runs fn up to policy.Attempts times (at least 1), waiting
// policy.Backoff through clock between failed attempts. A nil clock defaults to
// RealClock. If ctx is cancelled or clock.Sleep fails between attempts, the
// context or sleep error is returned immediately without invoking fn again.
func RetryWithBackoff(ctx context.Context, clock Clock, policy CleanupPolicy, fn func(context.Context) error) error {
	if clock == nil {
		clock = RealClock{}
	}
	attempts := max(policy.Attempts, 1)
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := clock.Sleep(ctx, policy.Backoff); err != nil {
				return err
			}
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// CheckRequirements verifies that every required capability is supported by the
// declared runtime adapter family, returning fault.UnsupportedCapability on the
// first missing capability.
func CheckRequirements(kind runtime.AdapterKind, required []runtime.Capability) error {
	family := runtime.CapabilitiesFor(kind)
	supported := make(map[runtime.Capability]struct{}, len(family))
	for _, capability := range family {
		supported[capability] = struct{}{}
	}
	for _, capability := range required {
		if _, ok := supported[capability]; !ok {
			return fault.New(fault.UnsupportedCapability,
				fmt.Sprintf("%s adapter does not support required capability %s", kind, capability))
		}
	}
	return nil
}
