package merge

import "github.com/optimiware/kodema/internal/apps/kit/servicekit"

// normalizeClaim canonicalizes the one time-valued claim field.
func normalizeClaim(claim Claim) Claim {
	claim.ExpiresAt = servicekit.NormalizedTime(claim.ExpiresAt)
	return claim
}

// claimsEqual compares claims by value with instant equality for the lease
// expiry, so a decoded claim still matches the live claim it was created from.
func claimsEqual(first, second Claim) bool {
	return first.Key == second.Key &&
		first.Owner == second.Owner &&
		first.OperationID == second.OperationID &&
		first.Generation == second.Generation &&
		first.ExpiresAt.Equal(second.ExpiresAt)
}

// normalizeRecord canonicalizes every time-valued field of one durable record.
func normalizeRecord(record CoordinationRecord) CoordinationRecord {
	record.Claim = normalizeClaim(record.Claim)
	record.UpdatedAt = servicekit.NormalizedTime(record.UpdatedAt)
	return record
}

// recordsEqual compares records by value with instant equality for their
// timestamps. It never compares time.Time structs directly because that also
// compares monotonic readings and locations.
func recordsEqual(first, second CoordinationRecord) bool {
	return claimsEqual(first.Claim, second.Claim) &&
		first.TenantID == second.TenantID &&
		first.State == second.State &&
		first.Request == second.Request &&
		first.Result == second.Result &&
		first.UpdatedAt.Equal(second.UpdatedAt) &&
		first.Released == second.Released &&
		first.Reconciled == second.Reconciled
}
