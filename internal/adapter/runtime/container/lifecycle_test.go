package container

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/fault"
)

type fakeClock struct {
	mu       sync.Mutex
	now      time.Time
	sleeps   []time.Duration
	onSleep  func(context.Context, time.Duration) error
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	hook := c.onSleep
	c.mu.Unlock()
	if hook != nil {
		return hook(ctx, d)
	}
	return ctx.Err()
}

func (c *fakeClock) recordedSleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func TestRealClock(t *testing.T) {
	var clk Clock = RealClock{}
	if clk.Now().IsZero() {
		t.Fatal("RealClock.Now returned zero time")
	}
	if err := clk.Sleep(context.Background(), 0); err != nil {
		t.Fatalf("Sleep(0) = %v; want nil", err)
	}
	if err := clk.Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("Sleep(1ms) = %v; want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := clk.Sleep(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(cancelled) = %v; want context.Canceled", err)
	}
	if err := clk.Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(cancelled, 0) = %v; want context.Canceled", err)
	}
}

func TestKeyedLockConcurrentAndCleanup(t *testing.T) {
	// Zero value must be immediately usable without constructor.
	var locks KeyedLock[string]

	const goroutines = 16
	const iterations = 50
	keys := []string{"k1", "k2", "k3"}
	active := make([]atomic.Int32, len(keys))
	counts := make([]int, len(keys))

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range iterations {
				idx := (worker + i) % len(keys)
				unlock := locks.Lock(keys[idx])
				if got := active[idx].Add(1); got != 1 {
					t.Errorf("key %s held concurrently by %d holders", keys[idx], got)
				}
				counts[idx]++
				active[idx].Add(-1)
				// Double-unlock must be idempotent and never corrupt ref counts.
				unlock()
				unlock()
			}
		}(g)
	}
	wg.Wait()

	total := 0
	for _, c := range counts {
		total += c
	}
	if total != goroutines*iterations {
		t.Fatalf("total increments = %d; want %d", total, goroutines*iterations)
	}

	locks.mu.Lock()
	remaining := len(locks.entries)
	locks.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("keyed lock retained %d entries after all unlocks; want 0", remaining)
	}
}

func TestKeyedLockDistinctKeysDoNotBlockEachOther(t *testing.T) {
	var locks KeyedLock[int]
	unlock1 := locks.Lock(1)
	defer unlock1()

	acquired := make(chan struct{})
	go func() {
		unlock2 := locks.Lock(2)
		close(acquired)
		unlock2()
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("distinct keys blocked each other")
	}
}

func TestCleanupPolicyValidateAndDefault(t *testing.T) {
	def := DefaultCleanupPolicy()
	if err := def.Validate(); err != nil {
		t.Fatalf("DefaultCleanupPolicy().Validate() = %v; want nil", err)
	}
	if def.Attempts < 1 || def.Backoff < 0 {
		t.Fatalf("unexpected default cleanup policy: %+v", def)
	}

	if err := (CleanupPolicy{Attempts: 1, Backoff: 0}).Validate(); err != nil {
		t.Fatalf("valid zero-backoff policy rejected: %v", err)
	}
	assertFaultClass(t, (CleanupPolicy{Attempts: 0, Backoff: time.Second}).Validate(), fault.InvalidInput)
	assertFaultClass(t, (CleanupPolicy{Attempts: -1, Backoff: time.Second}).Validate(), fault.InvalidInput)
	assertFaultClass(t, (CleanupPolicy{Attempts: 1, Backoff: -time.Millisecond}).Validate(), fault.InvalidInput)
}

func TestRetryWithBackoff(t *testing.T) {
	t.Run("succeeds on first attempt without sleeping", func(t *testing.T) {
		clk := newFakeClock()
		calls := 0
		err := RetryWithBackoff(context.Background(), clk, CleanupPolicy{Attempts: 3, Backoff: time.Second}, func(context.Context) error {
			calls++
			return nil
		})
		if err != nil {
			t.Fatalf("RetryWithBackoff = %v; want nil", err)
		}
		if calls != 1 {
			t.Fatalf("calls = %d; want 1", calls)
		}
		if len(clk.recordedSleeps()) != 0 {
			t.Fatalf("sleeps = %v; want none", clk.recordedSleeps())
		}
	})

	t.Run("succeeds on third attempt after two sleeps", func(t *testing.T) {
		clk := newFakeClock()
		calls := 0
		err := RetryWithBackoff(context.Background(), clk, CleanupPolicy{Attempts: 3, Backoff: 25 * time.Millisecond}, func(context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("temporary")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("RetryWithBackoff = %v; want nil", err)
		}
		if calls != 3 {
			t.Fatalf("calls = %d; want 3", calls)
		}
		sleeps := clk.recordedSleeps()
		if len(sleeps) != 2 || sleeps[0] != 25*time.Millisecond || sleeps[1] != 25*time.Millisecond {
			t.Fatalf("sleeps = %v; want [25ms 25ms]", sleeps)
		}
	})

	t.Run("exhausts attempts and returns last error", func(t *testing.T) {
		clk := newFakeClock()
		errFinal := errors.New("final failure")
		calls := 0
		err := RetryWithBackoff(context.Background(), clk, CleanupPolicy{Attempts: 3, Backoff: time.Second}, func(context.Context) error {
			calls++
			if calls == 3 {
				return errFinal
			}
			return errors.New("earlier failure")
		})
		if !errors.Is(err, errFinal) {
			t.Fatalf("err = %v; want %v", err, errFinal)
		}
		if calls != 3 || len(clk.recordedSleeps()) != 2 {
			t.Fatalf("calls=%d sleeps=%v; want 3 calls, 2 sleeps", calls, clk.recordedSleeps())
		}
	})

	t.Run("zero attempts still runs once", func(t *testing.T) {
		calls := 0
		err := RetryWithBackoff(context.Background(), nil, CleanupPolicy{Attempts: 0, Backoff: 0}, func(context.Context) error {
			calls++
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("err=%v calls=%d; want nil, 1", err, calls)
		}
	})

	t.Run("context cancelled during fn skips sleep and stops", func(t *testing.T) {
		clk := newFakeClock()
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		err := RetryWithBackoff(ctx, clk, CleanupPolicy{Attempts: 3, Backoff: time.Second}, func(context.Context) error {
			calls++
			cancel()
			return errors.New("boom")
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
		if calls != 1 || len(clk.recordedSleeps()) != 0 {
			t.Fatalf("calls=%d sleeps=%v; want 1 call, 0 sleeps", calls, clk.recordedSleeps())
		}
	})

	t.Run("sleep interruption returns sleep error immediately", func(t *testing.T) {
		clk := newFakeClock()
		ctx, cancel := context.WithCancel(context.Background())
		clk.onSleep = func(ctx context.Context, _ time.Duration) error {
			cancel()
			return ctx.Err()
		}
		calls := 0
		err := RetryWithBackoff(ctx, clk, CleanupPolicy{Attempts: 3, Backoff: time.Second}, func(context.Context) error {
			calls++
			return errors.New("retryable")
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
		if calls != 1 || len(clk.recordedSleeps()) != 1 {
			t.Fatalf("calls=%d sleeps=%v; want 1 call, 1 sleep", calls, clk.recordedSleeps())
		}
	})
}

func TestCheckRequirements(t *testing.T) {
	for _, kind := range runtime.AdapterKinds() {
		caps := runtime.CapabilitiesFor(kind)
		if err := CheckRequirements(kind, nil); err != nil {
			t.Fatalf("CheckRequirements(%s, nil) = %v; want nil", kind, err)
		}
		if err := CheckRequirements(kind, caps); err != nil {
			t.Fatalf("CheckRequirements(%s, supported) = %v; want nil", kind, err)
		}
	}

	assertFaultClass(t,
		CheckRequirements(runtime.AdapterProcess, []runtime.Capability{runtime.CapabilityContainer}),
		fault.UnsupportedCapability,
	)
	assertFaultClass(t,
		CheckRequirements(runtime.AdapterPodman, []runtime.Capability{runtime.CapabilityNetworkIsolation}),
		fault.UnsupportedCapability,
	)
	assertFaultClass(t,
		CheckRequirements(runtime.AdapterKubernetes, []runtime.Capability{runtime.CapabilityProcessGroupStop}),
		fault.UnsupportedCapability,
	)
	assertFaultClass(t,
		CheckRequirements(runtime.AdapterKind("unknown"), []runtime.Capability{runtime.CapabilityContainer}),
		fault.UnsupportedCapability,
	)
}
