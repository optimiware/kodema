package store

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/optimiware/kodema/internal/control/event"
	"github.com/optimiware/kodema/internal/control/receipt"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// ValidIdentity reports whether value is a bounded ASCII identity payload, the
// same rule the neutral id package applies to every typed identity.
func ValidIdentity(value string) bool {
	if value == "" || len(value) > id.MaxIDLength {
		return false
	}
	if value == "." || value == ".." {
		return false
	}
	for position := 0; position < len(value); position++ {
		character := value[position]
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case character == '-', character == '_':
		default:
			return false
		}
	}
	return true
}

// WriteRanks orders the tables a commit unit writes. Tables named by the
// declared lock order keep that order. A remaining schema table is placed
// immediately before the first lock-ordered table that references it, so an
// ownership parent always precedes its child inside one unit; a table nothing
// references is written last.
func WriteRanks(design Schema) map[string]float64 {
	lockOrder := make(map[string]int, len(design.LockOrder))
	for position, name := range design.LockOrder {
		lockOrder[name] = position
	}
	ranks := make(map[string]float64, len(design.Tables))
	for _, table := range design.Tables {
		if position, ok := lockOrder[table.Name]; ok {
			ranks[table.Name] = float64(position)
		}
	}
	for _, table := range design.Tables {
		if _, ok := ranks[table.Name]; ok {
			continue
		}
		first := -1
		for _, candidate := range design.Tables {
			for _, key := range candidate.ForeignKeys {
				if key.References != table.Name {
					continue
				}
				position, ok := lockOrder[candidate.Name]
				if !ok {
					continue
				}
				if first < 0 || position < first {
					first = position
				}
			}
		}
		if first < 0 {
			ranks[table.Name] = float64(len(lockOrder)) + 0.5
			continue
		}
		ranks[table.Name] = float64(first) - 0.5
	}
	return ranks
}

// ValidateAudit refuses an audit row outside the unit scope or with an unusable
// identity, actor, operation or time.
func ValidateAudit(scope tenancy.Scope, audit event.Audit) error {
	if audit.Scope != scope {
		return fault.New(fault.Forbidden, "audit record scope does not match the commit unit scope")
	}
	if _, err := id.ParseEventID(string(audit.ID)); err != nil {
		return fault.New(fault.InvalidInput, "audit record id is invalid")
	}
	if audit.Actor == "" {
		return fault.New(fault.InvalidInput, "audit record actor is empty")
	}
	if audit.Operation == "" {
		return fault.New(fault.InvalidInput, "audit record operation is empty")
	}
	if audit.At.IsZero() {
		return fault.New(fault.InvalidInput, "audit record time is zero")
	}
	return nil
}

// ValidateEvent refuses an event envelope outside the unit scope or with an
// unusable type, source, time or payload. The envelope rules themselves are the
// event contract's.
func ValidateEvent(scope tenancy.Scope, envelope event.Envelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	if envelope.Scope != scope {
		return fault.New(fault.Forbidden, "event scope does not match the commit unit scope")
	}
	return nil
}

// ValidateReceipt refuses a receipt whose identity, content hash or scope
// cannot protect the unit. Receipt identity is (tenant, project, principal,
// operation, caller key), and the project must be exactly the unit scope's
// project.
func ValidateReceipt(scope tenancy.Scope, record receipt.Record) error {
	if err := record.Key.Validate(); err != nil {
		return err
	}
	if _, err := id.ParseReceiptID(string(record.ID)); err != nil {
		return fault.New(fault.InvalidInput, "receipt id is invalid")
	}
	if _, err := ContentHashBytes(record.ContentHash); err != nil {
		return fault.New(fault.InvalidInput, "receipt content hash is not a sha256 fingerprint")
	}
	if record.At.IsZero() {
		return fault.New(fault.InvalidInput, "receipt time is zero")
	}
	if record.Key.TenantID != scope.TenantID || record.Key.ProjectID != scope.ProjectID {
		return fault.New(fault.Forbidden, "receipt identity does not match the commit unit scope")
	}
	return nil
}

// ContentHashBytes decodes a receipt content fingerprint into its byte form.
func ContentHashBytes(value string) ([]byte, error) {
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) {
		return nil, fmt.Errorf("receipt content hash has no sha256 prefix")
	}
	decoded, err := hex.DecodeString(value[len(prefix):])
	if err != nil {
		return nil, err
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf("receipt content hash is %d bytes; want 32", len(decoded))
	}
	return decoded, nil
}

// SameContentHash compares two receipt fingerprints byte-exactly, falling back
// to text equality only if a stored value is not a well-formed fingerprint.
func SameContentHash(left, right string) bool {
	leftBytes, leftErr := ContentHashBytes(left)
	rightBytes, rightErr := ContentHashBytes(right)
	if leftErr != nil || rightErr != nil {
		return left == right
	}
	return bytes.Equal(leftBytes, rightBytes)
}
