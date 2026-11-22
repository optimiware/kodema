package nats_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/shell/identity"
	"github.com/optimiware/kodema/internal/shell/integration/nats"
	"github.com/optimiware/kodema/internal/shell/service"
)

// outageConnector models a broker that stays down for the immediate reconnect
// after a discarded client and returns the replacement client on the next
// successful dial.
func outageConnector(first, second *fakeClient) *fakeConnector {
	return &fakeConnector{
		clients: []nats.Client{first, second},
		errs:    []error{nil, errBrokerDown},
	}
}

// TestResubscribeAfterFailedPublicationReconnect pins the outage resume: when a
// publication fails while the context is alive, the suspect client is
// discarded and the immediate reconnect also fails. The next successful dial
// (triggered by a later publication) must re-establish the already-open
// invalidation subscription on the replacement client, so a later invalidation
// is still delivered instead of silently orphaning the subscription.
func TestResubscribeAfterFailedPublicationReconnect(t *testing.T) {
	scope := testScope()
	human := humanPrincipal(scope)
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	resolver := &fakePrincipalResolver{byToken: map[string]identity.Principal{testCredential: human}}
	env := newBrokerEnv(t, brokerOptions{connector: connector, resolver: resolver, maxAttempts: 1})
	calls := &counter{}
	subscription, err := env.broker.Subscribe(context.Background(), subscribeRequest(testCredential, scope, loggingHandler(calls)))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = subscription.Close(context.Background()) }()
	oldSub := first.lastSubscription(t)
	first.failJetStream(errBrokerDown)

	// The outage fails the publication and the immediate reconnect.
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-outage"))), fault.DependencyUnavailable)
	if got := connector.callCount(); got != 2 {
		t.Fatalf("Connect calls during outage = %d; want 2 (initial dial and failed reconnect)", got)
	}

	// The next successful publication dials the replacement and must resume
	// the subscription on it.
	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-after-outage"))); err != nil {
		t.Fatalf("PublishEvent after outage: %v", err)
	}
	oldSub.waitClosed(t)
	wantSubject, err := nats.InvalidationSubject(scope)
	if err != nil {
		t.Fatalf("InvalidationSubject: %v", err)
	}
	newSub := subscriptionWithSubject(t, second, wantSubject)
	payload := mustJSON(t, validInvalidation(scope, id.PrincipalID("other-human"), service.ServiceID("other-service")))
	if err := newSub.deliver(context.Background(), payload); err != nil {
		t.Fatalf("delivery on resumed subscription: %v", err)
	}
	if got := calls.get(); got != 1 {
		t.Fatalf("handler calls = %d; want 1 after resubscribe", got)
	}
}

// TestResubscribeRevalidatesAuthorityOnResume pins fail-closed resume: a
// subscription whose authority was revoked while the broker was down is closed
// and never moved to the replacement client.
func TestResubscribeRevalidatesAuthorityOnResume(t *testing.T) {
	scope := testScope()
	human := humanPrincipal(scope)
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	resolver := &fakePrincipalResolver{byToken: map[string]identity.Principal{testCredential: human}}
	env := newBrokerEnv(t, brokerOptions{connector: connector, resolver: resolver, maxAttempts: 1})
	calls := &counter{}
	subscription, err := env.broker.Subscribe(context.Background(), subscribeRequest(testCredential, scope, loggingHandler(calls)))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = subscription.Close(context.Background()) }()
	oldSub := first.lastSubscription(t)
	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-outage-revoked"))), fault.DependencyUnavailable)

	// Authority is revoked while the broker is still down.
	resolver.Fail(testCredential, fault.New(fault.Unauthenticated, "credential revoked"))

	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-after-outage-revoked"))); err != nil {
		t.Fatalf("PublishEvent after outage: %v", err)
	}
	oldSub.waitClosed(t)
	if got := len(second.subscriptions()); got != 0 {
		t.Fatalf("replacement client subscriptions = %d; want 0 after revocation", got)
	}
	if got := calls.get(); got != 0 {
		t.Fatalf("handler calls = %d; want 0 after revocation", got)
	}
}

// TestResubscribeConcurrentPublishesReestablishOnce pins exactly-once resume:
// concurrent publications that share the first successful dial after an outage
// re-establish the subscription once, not once per caller.
func TestResubscribeConcurrentPublishesReestablishOnce(t *testing.T) {
	scope := testScope()
	human := humanPrincipal(scope)
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	resolver := &fakePrincipalResolver{byToken: map[string]identity.Principal{testCredential: human}}
	env := newBrokerEnv(t, brokerOptions{connector: connector, resolver: resolver, maxAttempts: 1})
	calls := &counter{}
	subscription, err := env.broker.Subscribe(context.Background(), subscribeRequest(testCredential, scope, loggingHandler(calls)))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = subscription.Close(context.Background()) }()
	oldSub := first.lastSubscription(t)
	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-outage-concurrent"))), fault.DependencyUnavailable)

	const count = 16
	start := make(chan struct{})
	errs := make(chan error, count)
	var wait sync.WaitGroup
	for index := range count {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			errs <- env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID(fmt.Sprintf("event-concurrent-%02d", index))))
		}(index)
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent PublishEvent after outage: %v", err)
		}
	}

	oldSub.waitClosed(t)
	wantSubject, err := nats.InvalidationSubject(scope)
	if err != nil {
		t.Fatalf("InvalidationSubject: %v", err)
	}
	subs := second.subscriptions()
	if len(subs) != 1 {
		t.Fatalf("replacement client subscriptions = %d; want exactly 1", len(subs))
	}
	if subs[0].subject != wantSubject {
		t.Fatalf("resumed subject = %q; want %q", subs[0].subject, wantSubject)
	}
	payload := mustJSON(t, validInvalidation(scope, id.PrincipalID("other-human"), service.ServiceID("other-service")))
	if err := subs[0].deliver(context.Background(), payload); err != nil {
		t.Fatalf("delivery on resumed subscription: %v", err)
	}
	if got := calls.get(); got != 1 {
		t.Fatalf("handler calls = %d; want 1 after exactly-once resubscribe", got)
	}
}

// TestResubscribeCloseDuringReestablish pins the shutdown race: Close may run
// while a pending resubscribe is revalidating. Close returns, the publication
// returns, the subscription is closed and no worker is left behind.
func TestResubscribeCloseDuringReestablish(t *testing.T) {
	scope := testScope()
	human := humanPrincipal(scope)
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	resolver := &fakePrincipalResolver{byToken: map[string]identity.Principal{testCredential: human}}
	env := newBrokerEnv(t, brokerOptions{connector: connector, resolver: resolver, maxAttempts: 1})
	calls := &counter{}
	subscription, err := env.broker.Subscribe(context.Background(), subscribeRequest(testCredential, scope, loggingHandler(calls)))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = subscription.Close(context.Background()) }()
	oldSub := first.lastSubscription(t)
	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-outage-close"))), fault.DependencyUnavailable)

	resumeStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	resolver.mu.Lock()
	resolver.onResolve = func(string) (identity.Principal, error) {
		once.Do(func() { close(resumeStarted) })
		<-release
		return human, nil
	}
	resolver.mu.Unlock()

	published := make(chan error, 1)
	go func() {
		published <- env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-close-race")))
	}()
	select {
	case <-resumeStarted:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("resubscribe did not revalidate")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := env.broker.Close(closeCtx); err != nil {
		close(release)
		t.Fatalf("Close during resubscribe: %v", err)
	}
	close(release)
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not return after Close")
	}
	oldSub.waitClosed(t)
}

// TestSubscribeLiveResubscribesAfterFailedReconnect pins that a live log
// subscription also resumes after an outage whose immediate reconnect failed:
// both the log and invalidation subscriptions move to the replacement client.
func TestSubscribeLiveResubscribesAfterFailedReconnect(t *testing.T) {
	scope := testScope()
	executionID := id.ExecutionID("exec-live-outage")
	first := newFakeClient()
	second := newFakeClient()
	connector := outageConnector(first, second)
	env := newBrokerEnv(t, brokerOptions{connector: connector, maxAttempts: 1})
	live, err := env.broker.SubscribeLive(context.Background(), scope, executionID)
	if err != nil {
		t.Fatalf("SubscribeLive: %v", err)
	}
	defer func() { _ = live.Close(context.Background()) }()
	logSubject, err := nats.LogSubject(scope, executionID)
	if err != nil {
		t.Fatalf("LogSubject: %v", err)
	}
	invalidationSubject, err := nats.InvalidationSubject(scope)
	if err != nil {
		t.Fatalf("InvalidationSubject: %v", err)
	}
	oldLog := subscriptionWithSubject(t, first, logSubject)
	first.failJetStream(errBrokerDown)
	requireClass(t, env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-live-outage"))), fault.DependencyUnavailable)

	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-live-after-outage"))); err != nil {
		t.Fatalf("PublishEvent after outage: %v", err)
	}
	oldLog.waitClosed(t)
	newLog := subscriptionWithSubject(t, second, logSubject)
	subscriptionWithSubject(t, second, invalidationSubject)
	chunk := validLiveChunk(executionID)
	if err := newLog.deliver(context.Background(), mustJSON(t, chunk)); err != nil {
		t.Fatalf("delivery on resumed live subscription: %v", err)
	}
	nextCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received, err := live.Next(nextCtx)
	if err != nil {
		t.Fatalf("Next after resubscribe: %v", err)
	}
	if received.Sequence != chunk.Sequence || string(received.Data) != string(chunk.Data) {
		t.Fatalf("Next returned %+v; want %+v", received, chunk)
	}
}

// gatedSubscribeClient wraps a fakeClient and pauses its Nth Subscribe call
// (defaulting to the first) at a barrier so tests can deterministically
// interleave a client reconnect right before registration under broker.mu.
type gatedSubscribeClient struct {
	*fakeClient
	gateCall int
	mu       sync.Mutex
	calls    int
	entered  chan struct{}
	release  chan struct{}
}

func (c *gatedSubscribeClient) Subscribe(ctx context.Context, subject nats.Subject, handler func(context.Context, []byte) error) (nats.Subscription, error) {
	sub, err := c.fakeClient.Subscribe(ctx, subject, handler)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.calls++
	target := c.gateCall
	if target <= 0 {
		target = 1
	}
	shouldGate := c.calls == target
	c.mu.Unlock()
	if shouldGate {
		close(c.entered)
		<-c.release
	}
	return sub, nil
}

// TestSubscribeReestablishesWhenClientChangesDuringSubscribe pins that if the
// broker reconnects to a replacement client while Subscribe is registering on
// the old client, the newly registered subscription is moved to the current
// client rather than staying orphaned on the discarded client.
func TestSubscribeReestablishesWhenClientChangesDuringSubscribe(t *testing.T) {
	scope := testScope()
	human := humanPrincipal(scope)
	first := &gatedSubscribeClient{
		fakeClient: newFakeClient(),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	second := newFakeClient()
	connector := &fakeConnector{clients: []nats.Client{first, second}}
	resolver := &fakePrincipalResolver{byToken: map[string]identity.Principal{testCredential: human}}
	env := newBrokerEnv(t, brokerOptions{connector: connector, resolver: resolver, maxAttempts: 2})
	calls := &counter{}

	type result struct {
		sub nats.Subscription
		err error
	}
	done := make(chan result, 1)
	go func() {
		sub, err := env.broker.Subscribe(context.Background(), subscribeRequest(testCredential, scope, loggingHandler(calls)))
		done <- result{sub: sub, err: err}
	}()

	select {
	case <-first.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not reach gated client.Subscribe")
	}

	// While Subscribe is paused on first, fail a publish so broker discards
	// first and reconnects to second.
	first.failJetStream(errBrokerDown)
	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-swap-sub"))); err != nil {
		close(first.release)
		t.Fatalf("PublishEvent reconnect: %v", err)
	}
	close(first.release)

	res := <-done
	if res.err != nil {
		t.Fatalf("Subscribe: %v", res.err)
	}
	defer func() { _ = res.sub.Close(context.Background()) }()

	wantSubject, err := nats.InvalidationSubject(scope)
	if err != nil {
		t.Fatalf("InvalidationSubject: %v", err)
	}
	oldSub := subscriptionWithSubject(t, first.fakeClient, wantSubject)
	oldSub.waitClosed(t)

	newSub := subscriptionWithSubject(t, second, wantSubject)
	payload := mustJSON(t, validInvalidation(scope, id.PrincipalID("other-human"), service.ServiceID("other-service")))
	if err := newSub.deliver(context.Background(), payload); err != nil {
		t.Fatalf("delivery on re-established subscription: %v", err)
	}
	if got := calls.get(); got != 1 {
		t.Fatalf("handler calls = %d; want 1", got)
	}
}

// TestSubscribeLiveReestablishesWhenClientChangesDuringSubscribe mirrors the
// mid-subscribe client swap check for SubscribeLive.
func TestSubscribeLiveReestablishesWhenClientChangesDuringSubscribe(t *testing.T) {
	scope := testScope()
	executionID := id.ExecutionID("exec-live-swap")
	first := &gatedSubscribeClient{
		fakeClient: newFakeClient(),
		gateCall:   2,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	second := newFakeClient()
	connector := &fakeConnector{clients: []nats.Client{first, second}}
	env := newBrokerEnv(t, brokerOptions{connector: connector, maxAttempts: 2})

	type result struct {
		sub interface{ Close(context.Context) error }
		err error
	}
	done := make(chan result, 1)
	go func() {
		sub, err := env.broker.SubscribeLive(context.Background(), scope, executionID)
		done <- result{sub: sub, err: err}
	}()

	select {
	case <-first.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SubscribeLive did not reach gated client.Subscribe")
	}

	first.failJetStream(errBrokerDown)
	if err := env.broker.PublishEvent(context.Background(), validEnvelope(scope, id.EventID("event-swap-live"))); err != nil {
		close(first.release)
		t.Fatalf("PublishEvent reconnect: %v", err)
	}
	close(first.release)

	res := <-done
	if res.err != nil {
		t.Fatalf("SubscribeLive: %v", res.err)
	}
	defer func() { _ = res.sub.Close(context.Background()) }()

	logSubject, err := nats.LogSubject(scope, executionID)
	if err != nil {
		t.Fatalf("LogSubject: %v", err)
	}
	oldLog := subscriptionWithSubject(t, first.fakeClient, logSubject)
	oldLog.waitClosed(t)
	subscriptionWithSubject(t, second, logSubject)
}

// TestBrokerCloseDoesNotBlockOnDialAndClosesDialedClient proves broker.mu is
// not held across Connect(ctx): Close returns promptly while a dial is in
// flight, and when the dial completes the newly dialed client is closed rather
// than leaked or installed.
func TestBrokerCloseDoesNotBlockOnDialAndClosesDialedClient(t *testing.T) {
	dialEntered := make(chan struct{})
	dialRelease := make(chan struct{})
	dialedClient := newFakeClient()

	broker, err := nats.NewBroker(nats.BrokerConfig{
		Connect: func(context.Context) (nats.Client, error) {
			close(dialEntered)
			<-dialRelease
			return dialedClient, nil
		},
		Clock:               newAutoClock(),
		Principal:           &fakePrincipalResolver{},
		Authorizer:          &fakeAuthorizer{},
		Sink:                &fakeSink{},
		MaxReconnectBackoff: 50 * time.Millisecond,
		IdleValidation:      time.Second,
		MaxPublishAttempts:  1,
	})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	publishErr := make(chan error, 1)
	go func() {
		publishErr <- broker.PublishEvent(context.Background(), validEnvelope(testScope(), id.EventID("event-dial-close")))
	}()

	select {
	case <-dialEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect was not invoked")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- broker.Close(context.Background())
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			close(dialRelease)
			t.Fatalf("Broker.Close during dial: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(dialRelease)
		t.Fatal("Broker.Close blocked while Connect was in flight")
	}

	close(dialRelease)
	select {
	case err := <-publishErr:
		requireClass(t, err, fault.DependencyUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("PublishEvent did not return after dial released")
	}
	if got := dialedClient.closeCount(); got != 1 {
		t.Fatalf("late dialed client Close calls = %d; want 1", got)
	}
}
