// Package storetest holds the store conformance scenarios every adapter must
// satisfy, so a rule the memory adapter enforces cannot be missing from the
// PostgreSQL adapter (or the reverse).
//
// The package imports only the neutral store seam and testing. Each adapter
// supplies its own backend through Run, so no adapter imports another and the
// scenarios never depend on a concrete harness.
package storetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/control/receipt"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// Backend is one adapter under test.
type Backend struct {
	Executor store.Executor
}

// Scope returns the project scope every scenario runs in.
func Scope() tenancy.Scope {
	return tenancy.Scope{TenantID: "tenant-parity", ProjectID: "project-parity"}
}

// Run executes the shared conformance scenarios against one adapter.
func Run(t *testing.T, newBackend func(*testing.T) Backend) {
	t.Run("refuses a business write to a publication table", func(t *testing.T) {
		backend := newBackend(t)
		for _, table := range []string{"receipts", "audit"} {
			_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
				Writes: []store.Write{{Table: table, ID: "row-parity-1", Insert: true, Values: map[string]any{}}},
			})
			requireClass(t, err, fault.InvalidInput, "business write to "+table)
		}
	})

	t.Run("refuses generic events writes", func(t *testing.T) {
		backend := newBackend(t)
		cases := []struct {
			name  string
			write store.Write
		}{
			{name: "insert", write: store.Write{Table: "events", ID: "event-parity-1", Insert: true, Values: map[string]any{"type": "task.created"}}},
			{name: "unpublishable state", write: store.Write{Table: "events", ID: "event-parity-1", Values: map[string]any{"publish_state": "claimed"}, Expect: map[string]any{"publish_state": "pending"}}},
			{name: "no guard", write: store.Write{Table: "events", ID: "event-parity-1", Values: map[string]any{"publish_state": "published"}}},
			{name: "extra column", write: store.Write{Table: "events", ID: "event-parity-1", Values: map[string]any{"publish_state": "published", "type": "task.created"}, Expect: map[string]any{"publish_state": "pending"}}},
		}
		for _, testCase := range cases {
			_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{Writes: []store.Write{testCase.write}})
			requireClass(t, err, fault.InvalidInput, "events "+testCase.name)
		}
	})

	t.Run("a root row outside the scope is forbidden before its shape is judged", func(t *testing.T) {
		backend := newBackend(t)
		// The write is both cross-tenant and shapeless (an update without its
		// expected version). Scope is checked first by every adapter, so the
		// refusal is forbidden, never invalid_input from one and forbidden
		// from the other.
		_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Writes: []store.Write{{Table: "tenants", ID: "tenant-other", Values: map[string]any{"name": "renamed"}}},
		})
		requireClass(t, err, fault.Forbidden, "cross-tenant root write")
	})

	t.Run("refuses an unknown column", func(t *testing.T) {
		backend := newBackend(t)
		_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Writes: []store.Write{{Table: "tasks", ID: "task-parity-1", Insert: true, Values: map[string]any{"no_such_column": "x"}}},
		})
		requireClass(t, err, fault.InvalidInput, "unknown column")
	})

	t.Run("refuses a receipt mode without a receipt", func(t *testing.T) {
		backend := newBackend(t)
		for _, mode := range []store.ReceiptMode{store.ReceiptInsertIncomplete, store.ReceiptComplete} {
			_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{ReceiptMode: mode})
			requireClass(t, err, fault.InvalidInput, string(mode)+" without a receipt")
		}
	})

	t.Run("completion keeps, sets or replaces the snapshot digest", func(t *testing.T) {
		first := [32]byte{1, 2, 3}
		second := [32]byte{9, 8, 7}
		cases := []struct {
			name       string
			inserted   *[32]byte
			completing *[32]byte
			want       *[32]byte
		}{
			{name: "keeps the inserted digest", inserted: &first, want: &first},
			{name: "sets an absent digest", completing: &second, want: &second},
			{name: "replaces the inserted digest", inserted: &first, completing: &second, want: &second},
			{name: "leaves an absent digest absent"},
		}
		for index, testCase := range cases {
			backend := newBackend(t)
			record := parityReceipt(t, index)
			record.Result = []byte("selection")
			record.SnapshotHash = testCase.inserted
			if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
				Receipt: &record, ReceiptMode: store.ReceiptInsertIncomplete,
			}); err != nil {
				t.Fatalf("%s: insert the incomplete receipt: %v", testCase.name, err)
			}
			completion := record
			completion.ID = id.ReceiptID(fmt.Sprintf("receipt-complete-%d", index))
			completion.At = record.At.Add(time.Minute)
			completion.Result = []byte("started")
			completion.SnapshotHash = testCase.completing
			unit := store.Unit{
				Receipt:       &completion,
				ReceiptMode:   store.ReceiptComplete,
				ReceiptExpect: &store.ReceiptExpect{ContentHash: record.ContentHash},
			}
			completed, err := backend.Executor.Execute(t.Context(), Scope(), unit)
			if err != nil {
				t.Fatalf("%s: complete the receipt: %v", testCase.name, err)
			}
			if completed.Replayed {
				t.Fatalf("%s: first completion reported Replayed=true; want false", testCase.name)
			}
			assertCompletedReceipt := func(label string, got receipt.Record) {
				t.Helper()
				if got.ID != record.ID {
					t.Errorf("%s (%s): receipt ID = %q; want original %q", testCase.name, label, got.ID, record.ID)
				}
				if !got.At.Equal(record.At) {
					t.Errorf("%s (%s): receipt At = %v; want original %v", testCase.name, label, got.At, record.At)
				}
				if string(got.Result) != "started" {
					t.Errorf("%s (%s): durable result = %q; want started", testCase.name, label, got.Result)
				}
				switch {
				case testCase.want == nil && got.SnapshotHash != nil:
					t.Errorf("%s (%s): durable snapshot digest = %x; want none", testCase.name, label, *got.SnapshotHash)
				case testCase.want != nil && (got.SnapshotHash == nil || *got.SnapshotHash != *testCase.want):
					t.Errorf("%s (%s): durable snapshot digest = %v; want %x", testCase.name, label, got.SnapshotHash, *testCase.want)
				}
			}
			assertCompletedReceipt("first completion", completed.Receipt)
			// A second completion replays the durable row, which is what a
			// retrying caller observes.
			replay, err := backend.Executor.Execute(t.Context(), Scope(), unit)
			if err != nil || !replay.Replayed {
				t.Fatalf("%s: replay = %+v, %v; want a replay of the completed receipt", testCase.name, replay, err)
			}
			assertCompletedReceipt("replay", replay.Receipt)
		}
	})

	t.Run("nil expectation matches only a NULL column", func(t *testing.T) {
		backend := newBackend(t)
		seedTaskWithoutExecution(t, backend)

		// The row's current_execution_id is NULL, so a nil expectation matches it.
		if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Writes: []store.Write{{
				Table:  "tasks",
				ID:     taskID,
				Values: map[string]any{"current_execution_id": "execution-parity"},
				Expect: map[string]any{"current_execution_id": nil, "lifecycle_version": int64(1)},
			}},
		}); err != nil {
			t.Fatalf("update guarded by a nil expectation: %v", err)
		}
		row, err := backend.Executor.Read(t.Context(), Scope(), "tasks", taskID)
		if err != nil {
			t.Fatalf("read updated task: %v", err)
		}
		if got := row.Values["current_execution_id"]; got != "execution-parity" {
			t.Fatalf("current_execution_id = %v; want execution-parity", got)
		}

		// The column is no longer NULL, so the same guard is now a conflict.
		_, err = backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Writes: []store.Write{{
				Table:  "tasks",
				ID:     taskID,
				Values: map[string]any{"current_execution_id": nil},
				Expect: map[string]any{"current_execution_id": nil},
			}},
		})
		requireClass(t, err, fault.VersionConflict, "stale nil expectation")
	})

	t.Run("version conflict on a stale expected version", func(t *testing.T) {
		backend := newBackend(t)
		seedService(t, backend)

		if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Writes: []store.Write{{
				Table:         "services",
				ID:            serviceID,
				Values:        map[string]any{"state": "disabled"},
				ExpectVersion: 1,
			}},
		}); err != nil {
			t.Fatalf("update at the observed version: %v", err)
		}
		_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Writes: []store.Write{{
				Table:         "services",
				ID:            serviceID,
				Values:        map[string]any{"state": "enabled"},
				ExpectVersion: 1,
			}},
		})
		requireClass(t, err, fault.VersionConflict, "stale expected version")
	})

	t.Run("absence guard enforces unit scope", func(t *testing.T) {
		backend := newBackend(t)
		tenantOnly := tenancy.Scope{TenantID: Scope().TenantID}

		// Project-scoped unit may guard its own project key and its own
		// tenant-wide (project_id: nil) key.
		for _, project := range []any{string(Scope().ProjectID), nil} {
			if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
				Locks: []store.Lock{{
					Table:        "memberships",
					ExpectAbsent: true,
					Key: map[string]any{
						"tenant_id":    string(Scope().TenantID),
						"project_id":   project,
						"principal_id": "principal-parity",
					},
				}},
			}); err != nil {
				t.Fatalf("in-scope absence guard (project=%v): %v", project, err)
			}
		}

		// Cross-tenant, cross-project, and project key in a tenant-scoped unit
		// are all forbidden.
		forbiddenCases := []struct {
			name    string
			scope   tenancy.Scope
			tenant  string
			project any
		}{
			{"foreign tenant in project scope", Scope(), "tenant-other", string(Scope().ProjectID)},
			{"foreign tenant nil project in project scope", Scope(), "tenant-other", nil},
			{"foreign project in project scope", Scope(), string(Scope().TenantID), "project-other"},
			{"project key in tenant scope", tenantOnly, string(Scope().TenantID), string(Scope().ProjectID)},
			{"foreign tenant in tenant scope", tenantOnly, "tenant-other", nil},
		}
		for _, tc := range forbiddenCases {
			_, err := backend.Executor.Execute(t.Context(), tc.scope, store.Unit{
				Locks: []store.Lock{{
					Table:        "memberships",
					ExpectAbsent: true,
					Key: map[string]any{
						"tenant_id":    tc.tenant,
						"project_id":   tc.project,
						"principal_id": "principal-parity",
					},
				}},
			})
			requireClass(t, err, fault.Forbidden, tc.name)
		}
	})

	t.Run("root row reads do not leak foreign project or tenant existence", func(t *testing.T) {
		backend := newBackend(t)
		tenantScope := tenancy.Scope{TenantID: Scope().TenantID}
		otherTenantScope := tenancy.Scope{TenantID: "tenant-other"}

		// Seed our tenant, another tenant, our project, and a sibling project
		// in the same tenant.
		if _, err := backend.Executor.Execute(t.Context(), tenantScope, store.Unit{
			Writes: []store.Write{
				{Table: "tenants", ID: string(Scope().TenantID), Insert: true, Values: map[string]any{
					"name": "acme", "version": int64(1), "derivation_key": []byte(`{"version":1}`), "created_at": parityTime(),
				}},
				{Table: "projects", ID: string(Scope().ProjectID), Insert: true, Values: map[string]any{
					"name": "p1", "version": int64(1), "created_at": parityTime(),
				}},
				{Table: "projects", ID: "project-sibling", Insert: true, Values: map[string]any{
					"name": "p2", "version": int64(1), "created_at": parityTime(),
				}},
			},
		}); err != nil {
			t.Fatalf("seed tenant and projects: %v", err)
		}
		if _, err := backend.Executor.Execute(t.Context(), otherTenantScope, store.Unit{
			Writes: []store.Write{{
				Table: "tenants", ID: "tenant-other", Insert: true, Values: map[string]any{
					"name": "other", "version": int64(1), "derivation_key": []byte(`{"version":1}`), "created_at": parityTime(),
				},
			}},
		}); err != nil {
			t.Fatalf("seed other tenant: %v", err)
		}

		// Reading an existing sibling project from project scope must return
		// NotFound, identical to a non-existent project id.
		_, err := backend.Executor.Read(t.Context(), Scope(), "projects", "project-sibling")
		requireClass(t, err, fault.NotFound, "existing sibling project from project scope")
		_, err = backend.Executor.Read(t.Context(), Scope(), "projects", "project-missing")
		requireClass(t, err, fault.NotFound, "missing project from project scope")

		// Reading an existing foreign tenant must return NotFound, identical to
		// a non-existent tenant id.
		_, err = backend.Executor.Read(t.Context(), Scope(), "tenants", "tenant-other")
		requireClass(t, err, fault.NotFound, "existing foreign tenant from project scope")
		_, err = backend.Executor.Read(t.Context(), tenantScope, "tenants", "tenant-other")
		requireClass(t, err, fault.NotFound, "existing foreign tenant from tenant scope")
	})

	t.Run("deadline lock requires exactly one row identity", func(t *testing.T) {
		backend := newBackend(t)
		for _, ids := range [][]string{nil, {}, {"action-1", "action-2"}} {
			_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
				Locks: []store.Lock{{
					Table:    "actions",
					IDs:      ids,
					Deadline: &store.DeadlineGuard{Column: "deadline"},
				}},
			})
			requireClass(t, err, fault.InvalidInput, fmt.Sprintf("deadline lock with %d ids", len(ids)))
		}
	})

	t.Run("same-table multi-row locks evaluate guards in row id order", func(t *testing.T) {
		backend := newBackend(t)
		for _, svc := range []struct{ id, name string }{
			{"service-a", "parity-a"},
			{"service-b", "parity-b"},
		} {
			if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
				Writes: []store.Write{{
					Table:  "services",
					ID:     svc.id,
					Insert: true,
					Values: map[string]any{
						"name":       svc.name,
						"roles":      []byte(`["engineer"]`),
						"state":      "enabled",
						"version":    int64(1),
						"created_at": parityTime(),
					},
				}},
			}); err != nil {
				t.Fatalf("seed %s: %v", svc.id, err)
			}
		}
		// Caller lists service-b (failing version guard) before service-a
		// (failing deadline guard). Both adapters sort same-table row locks by
		// row id, so service-a's deadline guard must be reported first.
		_, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
			Locks: []store.Lock{
				{
					Table:         "services",
					IDs:           []string{"service-b"},
					ExpectVersion: 99,
				},
				{
					Table:         "services",
					IDs:           []string{"service-a"},
					ExpectVersion: 1,
					Deadline:      &store.DeadlineGuard{Column: "token_expires_at"},
				},
			},
		})
		requireClass(t, err, fault.DeadlineExpired, "same-table lock ordering parity")
	})
}

const (
	taskID    = "task-parity-1"
	serviceID = "service-parity-1"
)

// seedTaskWithoutExecution inserts one task row whose current_execution_id is
// NULL. Only the columns both adapters need are supplied.
func seedTaskWithoutExecution(t *testing.T, backend Backend) {
	t.Helper()
	if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
		Writes: []store.Write{{
			Table:  "tasks",
			ID:     taskID,
			Insert: true,
			Values: map[string]any{
				"content_version":   int64(1),
				"lifecycle_version": int64(1),
				"state":             "open",
				"initiator_id":      "principal-parity",
				"title":             "parity",
				"created_at":        parityTime(),
			},
		}},
	}); err != nil {
		t.Fatalf("seed task: %v", err)
	}
}

// seedService inserts one enabled service row at version 1.
func seedService(t *testing.T, backend Backend) {
	t.Helper()
	if _, err := backend.Executor.Execute(t.Context(), Scope(), store.Unit{
		Writes: []store.Write{{
			Table:  "services",
			ID:     serviceID,
			Insert: true,
			Values: map[string]any{
				"name":       "parity",
				"roles":      []byte(`["engineer"]`),
				"state":      "enabled",
				"version":    int64(1),
				"created_at": parityTime(),
			},
		}},
	}); err != nil {
		t.Fatalf("seed service: %v", err)
	}
}

// parityReceipt returns one receipt record under its own caller key.
func parityReceipt(t *testing.T, index int) receipt.Record {
	t.Helper()
	fingerprint, err := receipt.Fingerprint(receipt.Content{MediaType: "application/json", Bytes: []byte(`{"parity":true}`)})
	if err != nil {
		t.Fatalf("fingerprint receipt content: %v", err)
	}
	return receipt.Record{
		ID: id.ReceiptID(fmt.Sprintf("receipt-parity-%d", index)),
		Key: receipt.Key{
			TenantID:    Scope().TenantID,
			ProjectID:   Scope().ProjectID,
			PrincipalID: "principal-parity",
			Operation:   "pipeline.start",
			CallerKey:   fmt.Sprintf("caller-parity-%d", index),
		},
		ContentHash: fingerprint,
		At:          parityTime(),
	}
}

func parityTime() time.Time {
	return time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
}

func requireClass(t *testing.T, err error, want fault.Class, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s was accepted; want %s", what, want)
	}
	if class, ok := fault.ClassOf(err); !ok || class != want {
		t.Fatalf("%s class = %v (%v); want %s", what, class, err, want)
	}
}
