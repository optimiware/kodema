package merge_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/apps/merge"
	"github.com/optimiware/kodema/internal/id"
)

var (
	_ servicekit.ServiceAdapter = (*merge.Adapter)(nil)
	_ merge.MergeCoordinator    = (*merge.Coordinator)(nil)
	_ merge.MergeRunner         = (*merge.Coordinator)(nil)
	_ merge.CoordinationStore   = (*merge.FileCoordinationStore)(nil)
	_ merge.MergeEffects        = (*fakeMergeEffects)(nil)
	_ servicekit.EventSource    = (*fakeEventSource)(nil)
	_ servicekit.Clock          = (*fakeClock)(nil)
	_ api.Handler               = (*fakeHandler)(nil)
)

func TestClaimGenerationAndOperationIdentity(t *testing.T) {
	claim := validClaim(t)
	if err := claim.Validate(); err != nil {
		t.Fatalf("valid claim refused: %v", err)
	}
	if claim.Expired(testNow) {
		t.Fatal("a claim expiring in the future reports expired")
	}
	if !claim.Expired(testNow.Add(2 * time.Minute)) {
		t.Fatal("an elapsed claim does not report expired")
	}

	for name, invalid := range map[string]merge.Claim{
		"empty owner":          {Key: validMergeKey(), OperationID: "op-1", Generation: 1, ExpiresAt: testNow},
		"empty operation id":   {Key: validMergeKey(), Owner: "w", Generation: 1, ExpiresAt: testNow},
		"invalid operation id": {Key: validMergeKey(), Owner: "w", OperationID: "op/../1", Generation: 1, ExpiresAt: testNow},
		"zero generation":      {Key: validMergeKey(), Owner: "w", OperationID: "op-1", ExpiresAt: testNow},
		"zero expiry":          {Key: validMergeKey(), Owner: "w", OperationID: "op-1", Generation: 1},
		"invalid key":          {Key: merge.MergeKey{}, Owner: "w", OperationID: "op-1", Generation: 1, ExpiresAt: testNow},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invalid.Validate(); err == nil {
				t.Fatalf("Claim.Validate accepted %+v", invalid)
			}
		})
	}
}

func TestMergeResultUncertainWithoutFabricatedObservations(t *testing.T) {
	key := validMergeKey()

	uncertain := merge.MergeResult{Key: key, State: merge.EffectUncertain}
	if err := uncertain.Validate(); err != nil {
		t.Fatalf("uncertain result without observations refused: %v", err)
	}
	partial := merge.MergeResult{Key: key, State: merge.EffectUncertain, ObservedBase: "base-1"}
	if err := partial.Validate(); err != nil {
		t.Fatalf("partially observed uncertain result refused: %v", err)
	}
	outputOnly := merge.MergeResult{Key: key, State: merge.EffectUncertain, ObservedOutput: "conflict while rebasing"}
	if err := outputOnly.Validate(); err != nil {
		t.Fatalf("output-only uncertain result refused: %v", err)
	}

	applied := merge.MergeResult{Key: key, State: merge.EffectApplied, ObservedBase: "base-1", ObservedCandidate: "cand-1", ObservedOutput: "Fast-forward"}
	if err := applied.Validate(); err != nil {
		t.Fatalf("applied result refused: %v", err)
	}
	if err := (merge.MergeResult{Key: key, State: merge.EffectApplied, ObservedBase: "base-1"}).Validate(); err == nil {
		t.Fatal("applied result without a candidate accepted")
	}
	if err := (merge.MergeResult{Key: key, State: merge.EffectNotStarted, ObservedOutput: "x"}).Validate(); err == nil {
		t.Fatal("not_started result with an observation accepted")
	}
	if err := (merge.MergeResult{Key: key, State: "maybe"}).Validate(); err == nil {
		t.Fatal("unknown result state accepted")
	}

	assertNoFieldMatching(t, reflect.TypeFor[merge.MergeResult](), "approval", "role", "reviewer", "authority", "decision")
}

func TestMergeObservationAndRequestValidation(t *testing.T) {
	key := validMergeKey()
	for name, observation := range map[string]merge.MergeObservation{
		"applied":     {State: merge.EffectApplied, Base: "base-1", Candidate: "cand-1", Output: "Fast-forward"},
		"not started": {State: merge.EffectNotStarted},
		"uncertain":   {State: merge.EffectUncertain},
		"partial":     {State: merge.EffectUncertain, Base: "base-1", Output: "conflict"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := observation.Validate(); err != nil {
				t.Fatalf("valid observation refused: %v", err)
			}
		})
	}
	for name, observation := range map[string]merge.MergeObservation{
		"applied without base":      {State: merge.EffectApplied, Candidate: "cand-1"},
		"applied without candidate": {State: merge.EffectApplied, Base: "base-1"},
		"not_started with base":     {State: merge.EffectNotStarted, Base: "base-1"},
		"not_started with output":   {State: merge.EffectNotStarted, Output: "x"},
		"unknown state":             {State: "maybe"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := observation.Validate(); err == nil {
				t.Fatalf("MergeObservation.Validate accepted %+v", observation)
			}
		})
	}

	if err := (merge.MergeRequest{Key: key, Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"}, Base: "base-1", Candidate: "cand-1"}).Validate(); err != nil {
		t.Fatalf("valid merge request refused: %v", err)
	}
	for name, request := range map[string]merge.MergeRequest{
		"invalid key":        {Base: "base-1", Candidate: "cand-1", Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"}},
		"missing assignment": {Key: key, Base: "base-1", Candidate: "cand-1"},
		"invalid assignment": {Key: key, Assignment: merge.MergeAssignment{ExecutionID: "../escape", ActionID: "action-1"}, Base: "base-1", Candidate: "cand-1"},
		"empty base":         {Key: key, Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"}, Candidate: "cand-1"},
		"empty candidate":    {Key: key, Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"}, Base: "base-1"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := request.Validate(); err == nil {
				t.Fatalf("MergeRequest.Validate accepted %+v", request)
			}
		})
	}

	assertNoFieldMatching(t, reflect.TypeFor[merge.MergeRequest](), "approval", "role", "reviewer", "authority")
}

func TestCoordinationRecordValidation(t *testing.T) {
	claim := validClaim(t)
	fresh := merge.CoordinationRecord{
		Claim:     claim,
		TenantID:  testTenant,
		State:     merge.EffectNotStarted,
		UpdatedAt: testNow,
	}
	if err := fresh.Validate(); err != nil {
		t.Fatalf("fresh record refused: %v", err)
	}

	intent := fresh
	intent.Request = merge.MergeRequest{Key: claim.Key, Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"}, Base: "base-1", Candidate: "cand-1"}
	if err := intent.Validate(); err != nil {
		t.Fatalf("intent record refused: %v", err)
	}

	applied := intent
	applied.State = merge.EffectApplied
	applied.Result = merge.MergeResult{Key: claim.Key, State: merge.EffectApplied, ObservedBase: "base-1", ObservedCandidate: "cand-1"}
	if err := applied.Validate(); err != nil {
		t.Fatalf("applied record refused: %v", err)
	}
	applied.Result = merge.MergeResult{}
	if err := applied.Validate(); err == nil {
		t.Fatal("applied record without a result accepted")
	}

	mismatched := intent
	mismatched.State = merge.EffectApplied
	mismatched.Result = merge.MergeResult{Key: claim.Key, State: merge.EffectUncertain}
	if err := mismatched.Validate(); err == nil {
		t.Fatal("record whose result state disagrees with its state accepted")
	}

	releasedUncertain := intent
	releasedUncertain.State = merge.EffectUncertain
	releasedUncertain.Result = merge.MergeResult{Key: claim.Key, State: merge.EffectUncertain}
	releasedUncertain.Released = true
	if err := releasedUncertain.Validate(); err == nil {
		t.Fatal("released uncertain record accepted")
	}

	for name, record := range map[string]merge.CoordinationRecord{
		"missing tenant":    {Claim: claim, State: merge.EffectNotStarted, UpdatedAt: testNow},
		"invalid tenant":    {Claim: claim, TenantID: id.TenantID("../escape"), State: merge.EffectNotStarted, UpdatedAt: testNow},
		"unknown state":     {Claim: claim, TenantID: testTenant, State: "maybe", UpdatedAt: testNow},
		"missing timestamp": {Claim: claim, TenantID: testTenant, State: merge.EffectNotStarted},
		"foreign request":   {Claim: claim, TenantID: testTenant, State: merge.EffectNotStarted, UpdatedAt: testNow, Request: merge.MergeRequest{Key: merge.MergeKey{Repository: "other/repo", Ref: "refs/heads/main"}, Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"}, Base: "a", Candidate: "b"}},
		"invalid claim":     {Claim: merge.Claim{}, TenantID: testTenant, State: merge.EffectNotStarted, UpdatedAt: testNow},
	} {
		t.Run(name, func(t *testing.T) {
			if err := record.Validate(); err == nil {
				t.Fatalf("CoordinationRecord.Validate accepted %+v", record)
			}
		})
	}
}

func TestMergeAdapterConfigRequiresMergeRunner(t *testing.T) {
	cfg := merge.AdapterConfig{
		ServiceID: testService,
		TenantID:  testTenant,
		ProjectID: testProject,
		Handler:   &fakeHandler{log: &callLog{}},
		Events:    &fakeEventSource{},
		Clock:     &fakeClock{now: testNow},
	}
	if _, err := merge.NewAdapter(cfg); err == nil {
		t.Fatal("NewAdapter accepted a merge coordinator without a merge dependency")
	}
	coordinator, err := merge.NewCoordinator(validCoordinatorConfig(
		newFakeCoordinationStore(&callLog{}), &fakeMergeEffects{log: &callLog{}}, &fakeClock{now: testNow}))
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	cfg.Merges = coordinator
	if _, err := merge.NewAdapter(cfg); err != nil {
		t.Fatalf("NewAdapter refused a configured merge coordinator: %v", err)
	}
}

func TestCoordinatorConfigRejectsIncompleteConfiguration(t *testing.T) {
	base := validCoordinatorConfig(&fakeCoordinationStore{log: &callLog{}}, &fakeMergeEffects{log: &callLog{}}, &fakeClock{now: testNow})
	if _, err := merge.NewCoordinator(base); err != nil {
		t.Fatalf("NewCoordinator with a complete config: %v", err)
	}

	for name, mutate := range map[string]func(*merge.CoordinatorConfig){
		"missing tenant":    func(config *merge.CoordinatorConfig) { config.TenantID = "" },
		"invalid tenant":    func(config *merge.CoordinatorConfig) { config.TenantID = "../escape" },
		"empty owner":       func(config *merge.CoordinatorConfig) { config.Owner = "" },
		"zero lease":        func(config *merge.CoordinatorConfig) { config.Lease = 0 },
		"negative lease":    func(config *merge.CoordinatorConfig) { config.Lease = -time.Second },
		"missing store":     func(config *merge.CoordinatorConfig) { config.Store = nil },
		"missing effects":   func(config *merge.CoordinatorConfig) { config.Effects = nil },
		"missing clock":     func(config *merge.CoordinatorConfig) { config.Clock = nil },
		"missing operation": func(config *merge.CoordinatorConfig) { config.NewOperationID = nil },
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			if _, err := merge.NewCoordinator(config); err == nil {
				t.Fatalf("NewCoordinator accepted a config with %s", name)
			}
		})
	}
}

func TestInvalidOperationIdentityRefusedAtClaim(t *testing.T) {
	store := &fakeCoordinationStore{log: &callLog{}}
	effects := &fakeMergeEffects{log: &callLog{}}
	config := validCoordinatorConfig(store, effects, &fakeClock{now: testNow})
	config.NewOperationID = func() (id.OperationID, error) { return "", errors.New("no operation id") }
	coordinator, err := merge.NewCoordinator(config)
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	if _, err := coordinator.Claim(context.Background(), validMergeKey()); err == nil {
		t.Fatal("Claim accepted a failing operation identity generator")
	}
	if _, ok, _ := store.Load(context.Background(), testTenant, validMergeKey()); ok {
		t.Fatal("a failed operation identity generator still created a claim")
	}
}

func assertNoFieldMatching(t *testing.T, structType reflect.Type, forbidden ...string) {
	t.Helper()
	for field := range structType.Fields() {
		name := strings.ToLower(field.Name)
		for _, fragment := range forbidden {
			if strings.Contains(name, fragment) {
				t.Fatalf("%s declares field %q; effect results carry no %s authority", structType, field.Name, fragment)
			}
		}
	}
}

func assertNoMethodMatching(t *testing.T, typeOf reflect.Type, forbidden ...string) {
	t.Helper()
	for method := range typeOf.Methods() {
		name := strings.ToLower(method.Name)
		for _, fragment := range forbidden {
			if strings.Contains(name, fragment) {
				t.Fatalf("%s exposes method %q; a merge coordinator never approves or reviews", typeOf, method.Name)
			}
		}
	}
}
