package postgres

import (
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/shell/store"
)

// tableMeta is the adapter's indexed view of one declared schema table. It is
// derived from store.SchemaDesign, never hand-maintained, so a schema change
// cannot drift from the statements the adapter issues.
type tableMeta struct {
	table           store.Table
	columnOrder     []string
	columns         map[string]store.Column
	hasTenant       bool
	hasProject      bool
	projectRequired bool
	hasVersion      bool
}

func (meta *tableMeta) hasColumn(name string) bool {
	_, ok := meta.columns[name]
	return ok
}

// schemaIndex is the deterministic lookup data the adapter builds once from the
// declared schema design.
type schemaIndex struct {
	tables     map[string]*tableMeta
	lockOrder  map[string]int
	writeRanks map[string]float64
}

var schema = buildSchemaIndex()

func buildSchemaIndex() schemaIndex {
	design := store.SchemaDesign()
	index := schemaIndex{
		tables:     make(map[string]*tableMeta, len(design.Tables)),
		lockOrder:  make(map[string]int, len(design.LockOrder)),
		writeRanks: make(map[string]float64, len(design.Tables)),
	}
	for position, name := range design.LockOrder {
		index.lockOrder[name] = position
	}
	for _, table := range design.Tables {
		meta := &tableMeta{
			table:       table,
			columnOrder: make([]string, 0, len(table.Columns)),
			columns:     make(map[string]store.Column, len(table.Columns)),
		}
		for _, column := range table.Columns {
			meta.columnOrder = append(meta.columnOrder, column.Name)
			meta.columns[column.Name] = column
		}
		meta.hasTenant = meta.hasColumn("tenant_id")
		meta.hasProject = meta.hasColumn("project_id")
		meta.projectRequired = meta.hasProject && !meta.columns["project_id"].Nullable
		meta.hasVersion = meta.hasColumn("version")
		index.tables[table.Name] = meta
	}
	index.writeRanks = store.WriteRanks(design)
	return index
}

// lookupTable returns the indexed metadata for one declared schema table.
func lookupTable(name string) (*tableMeta, bool) {
	meta, ok := schema.tables[name]
	return meta, ok
}

// writeRank returns the deterministic position one table is written at inside a
// commit unit. Every declared table has a rank.
func writeRank(name string) float64 {
	return schema.writeRanks[name]
}

// faultInvalid builds the invalid_input class.
func faultInvalid(message string) error {
	return fault.New(fault.InvalidInput, message)
}

// faultForbidden builds the forbidden class for a cross-scope or
// scope-mismatch refusal.
func faultForbidden(message string) error {
	return fault.New(fault.Forbidden, message)
}

// faultConflict builds the version_conflict class.
func faultConflict(message string) error {
	return fault.New(fault.VersionConflict, message)
}

// faultNotFound builds the not_found class.
func faultNotFound(message string) error {
	return fault.New(fault.NotFound, message)
}

// faultInternal builds the internal_failure class.
func faultInternal(message string) error {
	return fault.New(fault.InternalFailure, message)
}
