package definition_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func TestValidateCachesAndBudget(t *testing.T) {
	validNames := []string{"go-mod", "npm", "a", "pip_cache.v1", "0-cache.9_z", strings.Repeat("a", 63)}
	for _, name := range validNames {
		if err := definition.ValidateCacheName(name); err != nil {
			t.Errorf("ValidateCacheName(%q) failed: %v", name, err)
		}
	}

	invalidNames := []string{
		"",
		"Go-Mod",
		"../etc",
		"go/mod",
		`go\mod`,
		"go..mod",
		".hidden",
		"-leading-dash",
		"_leading_under",
		"has space",
		strings.Repeat("a", 64),
	}
	for _, name := range invalidNames {
		err := definition.ValidateCacheName(name)
		if err == nil {
			t.Errorf("ValidateCacheName(%q) succeeded; want refusal", name)
			continue
		}
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Errorf("ValidateCacheName(%q) class = %q (%v); want invalid_input", name, class, ok)
		}
	}

	if err := definition.ValidateCaches([]string{"go-mod", "go-mod"}); err == nil {
		t.Fatal("ValidateCaches accepted duplicate cache names")
	}
	tooMany := []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	if err := definition.ValidateCaches(tooMany); err == nil {
		t.Fatal("ValidateCaches accepted 9 caches; want max 8")
	}
	if err := definition.ValidateCaches(tooMany[:8]); err != nil {
		t.Fatalf("ValidateCaches refused 8 valid caches: %v", err)
	}

	// Budget validation.
	for _, valid := range []*definition.Budget{
		nil,
		{MaxTokens: 1000},
		{MaxCostUSD: 0.50},
		{MaxTokens: 50000, MaxCostUSD: 2.5},
	} {
		if err := definition.ValidateBudget(valid); err != nil {
			t.Errorf("ValidateBudget(%+v) failed: %v", valid, err)
		}
	}
	for _, invalid := range []*definition.Budget{
		{},
		{MaxTokens: -1},
		{MaxCostUSD: -0.01},
		{MaxCostUSD: math.NaN()},
		{MaxCostUSD: math.Inf(1)},
		{MaxTokens: definition.MaxBudgetTokens + 1},
		{MaxCostUSD: definition.MaxBudgetCostUSD + 1},
	} {
		if err := definition.ValidateBudget(invalid); err == nil {
			t.Errorf("ValidateBudget(%+v) succeeded; want refusal", invalid)
		}
	}

	// Pipeline Validate + Snapshot clone isolation for Caches and Budget.
	p := definition.Pipeline{
		Budget: &definition.Budget{MaxTokens: 10000, MaxCostUSD: 1.5},
		Actions: []definition.Action{{
			Name:    id.ActionName("build"),
			Timeout: time.Minute,
			Container: &definition.Container{
				Image:    "registry.example.com/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Commands: []string{"echo ok"},
				Caches:   []string{"go-mod", "npm"},
				Budget:   &definition.Budget{MaxTokens: 4000},
			},
		}},
	}
	if err := definition.Validate(p); err != nil {
		t.Fatalf("definition.Validate(valid caches+budget): %v", err)
	}
	snap := definition.NewSnapshot(definition.SnapshotInput{Pipeline: p})
	p.Budget.MaxTokens = 1
	p.Actions[0].Container.Caches[0] = "mutated"
	p.Actions[0].Container.Budget.MaxTokens = 1

	got := snap.Pipeline()
	if got.Budget == nil || got.Budget.MaxTokens != 10000 {
		t.Fatalf("snapshot pipeline budget mutated: %+v", got.Budget)
	}
	if len(got.Actions[0].Container.Caches) != 2 || got.Actions[0].Container.Caches[0] != "go-mod" {
		t.Fatalf("snapshot caches mutated: %+v", got.Actions[0].Container.Caches)
	}
	if got.Actions[0].Container.Budget == nil || got.Actions[0].Container.Budget.MaxTokens != 4000 {
		t.Fatalf("snapshot action budget mutated: %+v", got.Actions[0].Container.Budget)
	}
}

func TestValidateContainerCachesAndBudgetWrapWithoutDoublePrefix(t *testing.T) {
	basePipeline := func(mutate func(*definition.Container)) definition.Pipeline {
		container := &definition.Container{
			Image:    "registry.example.com/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Commands: []string{"echo ok"},
		}
		mutate(container)
		return definition.Pipeline{
			Actions: []definition.Action{{
				Name:      id.ActionName("build"),
				Timeout:   time.Minute,
				Container: container,
			}},
		}
	}

	cases := []struct {
		name   string
		mutate func(*definition.Container)
	}{
		{
			name: "invalid cache name",
			mutate: func(c *definition.Container) {
				c.Caches = []string{"Bad/Cache"}
			},
		},
		{
			name: "invalid budget",
			mutate: func(c *definition.Container) {
				c.Budget = &definition.Budget{MaxTokens: -5}
			},
		},
		{
			name: "invalid resources",
			mutate: func(c *definition.Container) {
				c.Resources = &definition.Resources{Memory: "not-a-quantity"}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := definition.Validate(basePipeline(tc.mutate))
			if err == nil {
				t.Fatalf("definition.Validate(%s) succeeded; want invalid_input", tc.name)
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("class = %q (%v); want invalid_input", class, ok)
			}
			msg, ok := fault.MessageOf(err)
			if !ok {
				t.Fatalf("MessageOf(%v) = false; want true", err)
			}
			if !strings.HasPrefix(msg, `action "build": `) {
				t.Fatalf("message = %q; want prefix %q", msg, `action "build": `)
			}
			if strings.Contains(msg, string(fault.InvalidInput)+":") {
				t.Fatalf("message %q double-prefixes %q", msg, fault.InvalidInput)
			}
			cause := errors.Unwrap(err)
			if cause == nil {
				t.Fatalf("errors.Unwrap(%v) = nil; want wrapped cause", err)
			}
			if class, ok := fault.ClassOf(cause); !ok || class != fault.InvalidInput {
				t.Fatalf("cause class = %q (%v); want invalid_input", class, ok)
			}
		})
	}
}
