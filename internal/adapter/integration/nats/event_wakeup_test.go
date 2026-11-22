package nats_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/integration/nats"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func TestBrokerSubscribeEventWakeup(t *testing.T) {
	client := newFakeClient()
	env := newBrokerEnv(t, brokerOptions{
		connector: &fakeConnector{clients: []nats.Client{client}},
	})
	scope := testScope()

	woken := 0
	sub, err := env.broker.SubscribeEventWakeup(context.Background(), scope, func() {
		woken++
	})
	if err != nil {
		t.Fatalf("SubscribeEventWakeup: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	wantSubject, err := nats.EventSubject(scope)
	if err != nil {
		t.Fatalf("EventSubject: %v", err)
	}
	fakeSub := client.lastSubscription(t)
	if fakeSub.subject != wantSubject {
		t.Fatalf("subscribed subject = %q; want %q", fakeSub.subject, wantSubject)
	}
	if err := fakeSub.deliver(context.Background(), []byte(`{"id":"evt-1"}`)); err != nil {
		t.Fatalf("deliver wakeup: %v", err)
	}
	if woken != 1 {
		t.Fatalf("wakeup count = %d; want 1", woken)
	}

	if _, err := env.broker.SubscribeEventWakeup(context.Background(), scope, nil); err == nil {
		t.Fatal("expected nil wakeup callback to fail")
	} else {
		requireClass(t, err, fault.InvalidInput)
	}
}

func TestBrokerEventWakeupResubscribesAfterReconnect(t *testing.T) {
	scope := testScope()
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	env := newBrokerEnv(t, brokerOptions{connector: connector, maxAttempts: 1})

	var woken atomic.Int32
	sub, err := env.broker.SubscribeEventWakeup(context.Background(), scope, func() {
		woken.Add(1)
	})
	if err != nil {
		t.Fatalf("SubscribeEventWakeup: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	wantSubject, err := nats.EventSubject(scope)
	if err != nil {
		t.Fatalf("EventSubject: %v", err)
	}
	oldSub := subscriptionWithSubject(t, first, wantSubject)

	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-wakeup-outage"))), fault.DependencyUnavailable)

	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-wakeup-after-outage"))); err != nil {
		t.Fatalf("PublishEvent after outage: %v", err)
	}
	oldSub.waitClosed(t)
	newSub := subscriptionWithSubject(t, second, wantSubject)
	if err := newSub.deliver(context.Background(), []byte(`{"id":"evt-2"}`)); err != nil {
		t.Fatalf("deliver on resumed wakeup sub: %v", err)
	}
	if got := woken.Load(); got != 1 {
		t.Fatalf("wakeup count after reconnect = %d; want 1", got)
	}
}

func TestBrokerEventWakeupSurvivesSubscribingAndResumeContextCancel(t *testing.T) {
	scope := testScope()
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	env := newBrokerEnv(t, brokerOptions{connector: connector, maxAttempts: 1})

	var woken atomic.Int32
	subCtx, cancelSub := context.WithCancel(context.Background())
	sub, err := env.broker.SubscribeEventWakeup(subCtx, scope, func() {
		woken.Add(1)
	})
	if err != nil {
		t.Fatalf("SubscribeEventWakeup: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	wantSubject, err := nats.EventSubject(scope)
	if err != nil {
		t.Fatalf("EventSubject: %v", err)
	}
	oldSub := subscriptionWithSubject(t, first, wantSubject)
	if err := oldSub.deliver(context.Background(), []byte(`{"id":"evt-1"}`)); err != nil {
		t.Fatalf("deliver before subscribe ctx cancel: %v", err)
	}
	cancelSub()
	if err := oldSub.deliver(context.Background(), []byte(`{"id":"evt-2"}`)); err != nil {
		t.Fatalf("deliver after subscribe ctx cancel: %v", err)
	}
	if got := woken.Load(); got != 2 {
		t.Fatalf("wakeup count after subscribe ctx cancel = %d; want 2", got)
	}

	// Trigger reconnect using a short-lived publisher context, then cancel it
	// before delivering on the resumed subscription.
	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-wakeup-outage"))), fault.DependencyUnavailable)

	pubCtx, cancelPub := context.WithCancel(context.Background())
	if err := env.broker.PublishEvent(pubCtx, validEnvelope(scope, id.EventID("event-wakeup-after-outage"))); err != nil {
		t.Fatalf("PublishEvent after outage: %v", err)
	}
	cancelPub()
	oldSub.waitClosed(t)
	newSub := subscriptionWithSubject(t, second, wantSubject)
	if err := newSub.deliver(context.Background(), []byte(`{"id":"evt-3"}`)); err != nil {
		t.Fatalf("deliver after resume ctx cancel: %v", err)
	}
	if got := woken.Load(); got != 3 {
		t.Fatalf("wakeup count after reconnect + resume ctx cancel = %d; want 3", got)
	}
}

func TestBrokerEventWakeupRetriesAfterTransientResubscribeFailure(t *testing.T) {
	scope := testScope()
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	env := newBrokerEnv(t, brokerOptions{connector: connector, maxAttempts: 1})

	var woken atomic.Int32
	sub, err := env.broker.SubscribeEventWakeup(context.Background(), scope, func() {
		woken.Add(1)
	})
	if err != nil {
		t.Fatalf("SubscribeEventWakeup: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	wantSubject, err := nats.EventSubject(scope)
	if err != nil {
		t.Fatalf("EventSubject: %v", err)
	}
	oldSub := subscriptionWithSubject(t, first, wantSubject)

	// Force disconnect on first client, and make the initial re-subscribe on
	// second fail transiently.
	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-wakeup-outage-retry"))), fault.DependencyUnavailable)

	second.mu.Lock()
	second.subscribeErr = errBrokerDown
	second.mu.Unlock()
	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-wakeup-reconnect-transient"))); err != nil {
		t.Fatalf("PublishEvent after outage: %v", err)
	}
	if len(second.subscriptions()) != 0 {
		t.Fatalf("second subscriptions = %d; want 0 while subscribeErr is set", len(second.subscriptions()))
	}

	// Clear transient subscribe error; the next broker operation must retry and
	// re-establish the wakeup subscription instead of leaving it permanently dead.
	second.mu.Lock()
	second.subscribeErr = nil
	second.mu.Unlock()
	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-wakeup-retry-succeeds"))); err != nil {
		t.Fatalf("PublishEvent retry: %v", err)
	}
	oldSub.waitClosed(t)
	newSub := subscriptionWithSubject(t, second, wantSubject)
	if err := newSub.deliver(context.Background(), []byte(`{"id":"evt-recovered"}`)); err != nil {
		t.Fatalf("deliver after transient resubscribe recovery: %v", err)
	}
	if got := woken.Load(); got != 1 {
		t.Fatalf("wakeup count after transient resubscribe recovery = %d; want 1", got)
	}
}

func TestBrokerCloseShutsDownEventWakeupAndRefusesNew(t *testing.T) {
	scope := testScope()
	client := newFakeClient()
	env := newBrokerEnv(t, brokerOptions{
		connector: &fakeConnector{clients: []nats.Client{client}},
	})

	sub, err := env.broker.SubscribeEventWakeup(context.Background(), scope, func() {})
	if err != nil {
		t.Fatalf("SubscribeEventWakeup: %v", err)
	}
	fakeSub := client.lastSubscription(t)

	if err := env.broker.Close(context.Background()); err != nil {
		t.Fatalf("Broker.Close: %v", err)
	}
	fakeSub.waitClosed(t)

	// Closing the handle after Broker.Close is idempotent.
	if err := sub.Close(context.Background()); err != nil {
		t.Fatalf("sub.Close after Broker.Close: %v", err)
	}

	if _, err := env.broker.SubscribeEventWakeup(context.Background(), scope, func() {}); err == nil {
		t.Fatal("expected SubscribeEventWakeup after Broker.Close to fail")
	} else {
		requireClass(t, err, fault.DependencyUnavailable)
	}
}
