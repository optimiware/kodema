package nats

import (
	"context"
	"sync"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/shell/tenancy"
)

// SubscribeEventWakeup opens a lightweight, scope-qualified broker wakeup
// subscription on EventSubject(scope). It invokes onWakeup whenever any event
// message is published for that scope so server-side HTTP SSE streams can drain
// newly committed rows from the durable events table immediately, including
// across multi-replica deployments.
//
// Broker messages are purely an acceleration signal: payloads are ignored here,
// and callers remain responsible for authenticating/authorizing subscribers and
// reading committed rows from the durable event table.
//
// The subscription outlives the caller's ctx (which bounds only initial dial
// and setup) and remains active until Close or Broker.Close. Callers must not
// invoke Close from inside onWakeup, because Close joins in-flight callbacks.
func (broker *Broker) SubscribeEventWakeup(ctx context.Context, scope tenancy.Scope, onWakeup func()) (Subscription, error) {
	if onWakeup == nil {
		return nil, fault.New(fault.InvalidInput, "event wakeup handler is nil")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fault.Wrap(fault.DeadlineExpired, "event wakeup subscription was canceled", err)
	}
	subject, err := EventSubject(scope)
	if err != nil {
		return nil, err
	}
	client, err := broker.ensureClient(ctx)
	if err != nil {
		return nil, dependencyError("event wakeup subscription failed", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fault.Wrap(fault.DeadlineExpired, "event wakeup subscription was canceled", err)
	}

	wakeup := &wakeupSubscription{
		broker:   broker,
		subject:  subject,
		onWakeup: onWakeup,
		done:     make(chan struct{}),
	}
	wakeup.idle = sync.NewCond(&wakeup.mu)

	// Use the broker-lifetime context so the driver's subscription handler
	// context stays alive after the subscribing request ends (e.g. when one
	// SSE stream disconnects while sibling streams share this scope wakeup).
	clientSub, err := client.Subscribe(broker.baseCtx, subject, wakeup.handle)
	if err != nil {
		return nil, fault.Wrap(fault.DependencyUnavailable, "event wakeup subscription failed", err)
	}

	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		_ = clientSub.Close(context.Background())
		return nil, fault.New(fault.DependencyUnavailable, "broker adapter is closed")
	}
	wakeup.mu.Lock()
	wakeup.client = client
	wakeup.clientSub = clientSub
	wakeup.mu.Unlock()
	broker.wakeupSubscriptions[wakeup] = struct{}{}
	clientChanged := broker.client != client
	if clientChanged {
		broker.resubscribePending = true
	}
	resumeNow := clientChanged && broker.client != nil
	broker.mu.Unlock()

	if resumeNow {
		broker.resumeSubscriptions(ctx)
	}
	return wakeup, nil
}

// wakeupSubscription is one open Core NATS event-wakeup subscription tracked
// by the broker so reconnects re-establish it and Broker.Close shuts it down.
type wakeupSubscription struct {
	broker   *Broker
	subject  Subject
	onWakeup func()

	mu        sync.Mutex
	idle      *sync.Cond
	closed    bool
	inFlight  int
	client    Client
	clientSub Subscription
	done      chan struct{}
}

// Close releases the wakeup subscription, closes its client subscription, and
// joins any in-flight wakeup callback. It is idempotent and must not be called
// from inside onWakeup.
func (wakeup *wakeupSubscription) Close(context.Context) error {
	wakeup.shutdown()
	wakeup.wait()
	return nil
}

func (wakeup *wakeupSubscription) handle(_ context.Context, _ []byte) error {
	wakeup.mu.Lock()
	if wakeup.closed {
		wakeup.mu.Unlock()
		return fault.New(fault.DependencyUnavailable, "event wakeup subscription is closed")
	}
	wakeup.inFlight++
	wakeup.mu.Unlock()
	defer wakeup.finishDelivery()

	wakeup.onWakeup()
	return nil
}

// reestablish moves the wakeup subscription onto client using the broker's
// base context. On a transient subscribe error it leaves the wakeup registered
// and returns false so the broker retries on the next operation instead of
// permanently killing the shared scope wakeup.
func (wakeup *wakeupSubscription) reestablish(client Client) bool {
	wakeup.mu.Lock()
	if wakeup.closed || wakeup.client == client {
		wakeup.mu.Unlock()
		return true
	}
	wakeup.mu.Unlock()
	clientSub, err := client.Subscribe(wakeup.broker.baseCtx, wakeup.subject, wakeup.handle)
	if err != nil {
		return false
	}
	wakeup.mu.Lock()
	if wakeup.closed {
		wakeup.mu.Unlock()
		_ = clientSub.Close(context.Background())
		return true
	}
	previous := wakeup.clientSub
	wakeup.client = client
	wakeup.clientSub = clientSub
	wakeup.mu.Unlock()
	if previous != nil {
		_ = previous.Close(context.Background())
	}
	return true
}

func (wakeup *wakeupSubscription) shutdown() {
	wakeup.mu.Lock()
	if wakeup.closed {
		wakeup.mu.Unlock()
		return
	}
	wakeup.closed = true
	clientSub := wakeup.clientSub
	wakeup.client = nil
	wakeup.clientSub = nil
	close(wakeup.done)
	wakeup.mu.Unlock()

	wakeup.broker.unregisterWakeup(wakeup)
	if clientSub != nil {
		_ = clientSub.Close(context.Background())
	}
}

func (wakeup *wakeupSubscription) wait() {
	<-wakeup.done
	wakeup.mu.Lock()
	for wakeup.inFlight > 0 {
		wakeup.idle.Wait()
	}
	wakeup.mu.Unlock()
}

func (wakeup *wakeupSubscription) finishDelivery() {
	wakeup.mu.Lock()
	wakeup.inFlight--
	if wakeup.inFlight == 0 {
		wakeup.idle.Broadcast()
	}
	wakeup.mu.Unlock()
}
