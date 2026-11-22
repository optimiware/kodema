// Regression tests: the controller must reclassify a zero-row lock/write by
// scope, not by bare existence, so a same-scope stale version is a
// version_conflict while a row in another scope stays forbidden.
package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/shell/store"
	"github.com/optimiware/kodema/internal/shell/tenancy"
)

func newControllerRegressionHarness(t *testing.T, rows []seamSeedRow) *Controller {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	db := newEmuDB("kodema_controller", now)
	seedEmulator(t, db, seamSeed{Rows: rows}, now)
	return NewController(db)
}

func regressionRows(actionProject string, actionVersion uint64) []seamSeedRow {
	return []seamSeedRow{
		{
			Table: "executions", TenantID: "tenant-1", ProjectID: "project-1", ID: "execution-1", Version: 3,
			Values: map[string]any{"state": "running", "principal_id": "principal-1"},
		},
		{
			Table: "actions", TenantID: "tenant-1", ProjectID: actionProject, ID: "action-1", Version: actionVersion,
			Values: map[string]any{
				"execution_id": "execution-1",
				"state":        "running",
				"index":        0,
				"deadline":     "2026-01-01T01:00:00Z",
			},
		},
	}
}

func assertRegressionFaultClass(t *testing.T, err error, want fault.Class) {
	t.Helper()
	if err == nil {
		t.Fatalf("operation succeeded; want fault class %q", want)
	}
	class, ok := fault.ClassOf(err)
	if !ok || class != want {
		t.Fatalf("error = %v (class=%q); want %q", err, class, want)
	}
}

// A same-scope row at a stale version is a version_conflict, never forbidden.
// A bare-existence reclassify would return forbidden because the row exists.
func TestControllerWriteStaleVersionIsVersionConflict(t *testing.T) {
	controller := newControllerRegressionHarness(t, regressionRows("project-1", 5))
	unit := store.Unit{Writes: []store.Write{{
		Table: "actions", ID: "action-1", ExpectVersion: 2,
		Values: map[string]any{"state": "succeeded"},
	}}}
	_, err := controller.Execute(context.Background(), "execution-1", unit)
	assertRegressionFaultClass(t, err, fault.VersionConflict)
}

// A stale guarded lock (the lock matches scope, the version guard does not) is
// also a same-scope version_conflict.
func TestControllerLockGuardStaleVersionIsVersionConflict(t *testing.T) {
	controller := newControllerRegressionHarness(t, regressionRows("project-1", 5))
	unit := store.Unit{Locks: []store.Lock{{
		Table: "actions", IDs: []string{"action-1"}, ExpectVersion: 2,
	}}}
	_, err := controller.Execute(context.Background(), "execution-1", unit)
	assertRegressionFaultClass(t, err, fault.VersionConflict)
}

// A deadline-guarded write whose same-scope row is not at the expected version
// misses the deadline query and must report version_conflict.
func TestControllerDeadlineWriteStaleVersionIsVersionConflict(t *testing.T) {
	controller := newControllerRegressionHarness(t, regressionRows("project-1", 5))
	unit := store.Unit{Writes: []store.Write{{
		Table: "actions", ID: "action-1", ExpectVersion: 2,
		Deadline: &store.DeadlineGuard{Column: "deadline"},
		Values:   map[string]any{"state": "succeeded"},
	}}}
	_, err := controller.Execute(context.Background(), "execution-1", unit)
	assertRegressionFaultClass(t, err, fault.VersionConflict)
}

// A same-scope missing row is a version_conflict, not forbidden.
func TestControllerMissingRowIsVersionConflict(t *testing.T) {
	controller := newControllerRegressionHarness(t, regressionRows("project-1", 5))
	unit := store.Unit{Writes: []store.Write{{
		Table: "actions", ID: "action-9", ExpectVersion: 2,
		Values: map[string]any{"state": "succeeded"},
	}}}
	_, err := controller.Execute(context.Background(), "execution-1", unit)
	assertRegressionFaultClass(t, err, fault.VersionConflict)
}

// A row that exists in a different scope is still forbidden, for both a write
// and a lock.
func TestControllerOtherScopeRowIsForbidden(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		controller := newControllerRegressionHarness(t, regressionRows("project-2", 1))
		unit := store.Unit{Writes: []store.Write{{
			Table: "actions", ID: "action-1", ExpectVersion: 1,
			Values: map[string]any{"state": "succeeded"},
		}}}
		_, err := controller.Execute(context.Background(), "execution-1", unit)
		assertRegressionFaultClass(t, err, fault.Forbidden)
	})
	t.Run("lock", func(t *testing.T) {
		controller := newControllerRegressionHarness(t, regressionRows("project-2", 1))
		unit := store.Unit{Locks: []store.Lock{{
			Table: "actions", IDs: []string{"action-1"}, ExpectVersion: 1,
		}}}
		_, err := controller.Execute(context.Background(), "execution-1", unit)
		assertRegressionFaultClass(t, err, fault.Forbidden)
	})
}

// The scope predicate must treat a NULL project as a value: a tenant-scoped
// derived scope matches a tenant-wide row, while a project scope does not.
func TestControllerReclassifyTenantWideProject(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	db := newEmuDB("kodema_controller", now)
	seedEmulator(t, db, seamSeed{Rows: []seamSeedRow{{
		Table: "events", TenantID: "tenant-1", ProjectID: "", ID: "event-1", Version: 1,
		Values: map[string]any{"type": "project.created", "publish_sequence": 1},
	}}}, now)
	ctx := context.Background()
	tx, err := beginUnscoped(ctx, db, false)
	if err != nil {
		t.Fatalf("begin unscoped: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tenantWide := tenancy.Scope{TenantID: "tenant-1"}
	if err := controllerReclassify(ctx, tx, tenantWide, "events", "event-1"); err == nil {
		t.Fatal("tenant-wide row in the same tenant scope reclassified as forbidden; want version_conflict")
	} else {
		assertRegressionFaultClass(t, err, fault.VersionConflict)
	}

	project := tenancy.Scope{TenantID: "tenant-1", ProjectID: "project-1"}
	if err := controllerReclassify(ctx, tx, project, "events", "event-1"); err == nil {
		t.Fatal("tenant-wide row outside the project scope reclassified as a conflict; want forbidden")
	} else {
		assertRegressionFaultClass(t, err, fault.Forbidden)
	}
}

// checkWriteDeadline must close its rows iterator and check rows.Err() before
// invoking reclassify so a strict connection driver (like pgx) never fails with
// conn busy when reclassify issues its own SELECT.
func TestCheckWriteDeadlineClosesRowsBeforeReclassify(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scope := testProjectScope()
	planned, err := planWrite(scope, store.Write{
		Table:         "actions",
		ID:            "action-1",
		ExpectVersion: 1,
		Values:        map[string]any{"state": "succeeded"},
		Deadline:      &store.DeadlineGuard{Column: "deadline"},
	})
	if err != nil {
		t.Fatalf("planWrite: %v", err)
	}

	t.Run("closes empty deadline rows before reclassify query", func(t *testing.T) {
		db := &strictCloseDB{
			queryFn: func(sql string, _ []any) (*strictCloseRows, error) {
				switch {
				case sql == transactionClockSQL:
					return &strictCloseRows{inner: &fakeRows{rows: [][]any{{now}}, index: -1}}, nil
				case strings.Contains(strings.ToLower(sql), "not ("):
					// Reclassify finds the row in another scope -> Forbidden.
					return &strictCloseRows{inner: &fakeRows{rows: [][]any{{"action-1"}}, index: -1}}, nil
				default:
					// Deadline guard query returns 0 rows.
					return &strictCloseRows{inner: &fakeRows{index: -1}}, nil
				}
			},
		}
		tx, err := beginUnscoped(context.Background(), db, false)
		if err != nil {
			t.Fatalf("beginUnscoped: %v", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()

		err = checkWriteDeadline(context.Background(), tx, planned, func(ctx context.Context, tx store.Transaction, table, id string) error {
			return controllerReclassify(ctx, tx, scope, table, id)
		})
		assertRegressionFaultClass(t, err, fault.Forbidden)
	})

	t.Run("surfaces rows close error before calling reclassify", func(t *testing.T) {
		closeErr := fault.New(fault.DependencyUnavailable, "deadline rows close failed")
		reclassified := false
		db := &strictCloseDB{
			queryFn: func(sql string, _ []any) (*strictCloseRows, error) {
				if sql == transactionClockSQL {
					return &strictCloseRows{inner: &fakeRows{rows: [][]any{{now}}, index: -1}}, nil
				}
				return &strictCloseRows{inner: &fakeRows{index: -1}, closeErr: closeErr}, nil
			},
		}
		tx, err := beginUnscoped(context.Background(), db, false)
		if err != nil {
			t.Fatalf("beginUnscoped: %v", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()

		err = checkWriteDeadline(context.Background(), tx, planned, func(context.Context, store.Transaction, string, string) error {
			reclassified = true
			return nil
		})
		assertRegressionFaultClass(t, err, fault.DependencyUnavailable)
		if reclassified {
			t.Fatal("reclassify was called after rows.Close failed")
		}
	})
}
