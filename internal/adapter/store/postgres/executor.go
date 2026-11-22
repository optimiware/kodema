package postgres

import (
	"context"
	"fmt"

	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
)

var _ store.Executor = (*Store)(nil)

// Execute applies one complete commit unit through the store's own connector.
// The neutral store.Unit carries no SQL; this adapter renders and orders the
// locks, writes, receipt, audit rows and event publication intent inside one
// scoped transaction.
func (s *Store) Execute(ctx context.Context, scope tenancy.Scope, unit store.Unit) (store.UnitResult, error) {
	return ExecuteUnit(ctx, s, scope, unit)
}

// Read reads one row by identity inside a scoped read-only transaction. Inputs
// are validated before opening a transaction so malformed reads never touch the
// pool. The transaction is completed with a commit so the transaction-local
// scope resets with the transaction; a failed read rolls the transaction back
// and returns the original cause.
func (s *Store) Read(ctx context.Context, scope tenancy.Scope, table, id string) (store.Row, error) {
	if err := ctx.Err(); err != nil {
		return store.Row{}, err
	}
	meta, ok := lookupTable(table)
	if !ok {
		return store.Row{}, faultInvalid(fmt.Sprintf("unknown table %q", table))
	}
	if err := scope.Validate(); err != nil {
		return store.Row{}, err
	}
	if !store.ValidIdentity(id) {
		return store.Row{}, faultInvalid(fmt.Sprintf("row id %q is not a valid identity", id))
	}
	if _, err := scopedReadQuery(meta, scope, id); err != nil {
		return store.Row{}, err
	}
	tx, err := s.beginRead(ctx, scope)
	if err != nil {
		return store.Row{}, err
	}
	row, err := ReadRow(ctx, tx, scope, table, id)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return store.Row{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.Row{}, err
	}
	return row, nil
}
