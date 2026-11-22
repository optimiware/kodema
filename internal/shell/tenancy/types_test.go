package tenancy_test

import (
	"testing"

	"github.com/optimiware/kodema/internal/shell/tenancy"
)

func TestScopeContains(t *testing.T) {
	cases := []struct {
		name   string
		owner  tenancy.Scope
		target tenancy.Scope
		want   bool
	}{
		{
			name:   "tenant covers tenant",
			owner:  tenancy.Scope{TenantID: "t1"},
			target: tenancy.Scope{TenantID: "t1"},
			want:   true,
		},
		{
			name:   "tenant covers project in same tenant",
			owner:  tenancy.Scope{TenantID: "t1"},
			target: tenancy.Scope{TenantID: "t1", ProjectID: "p1"},
			want:   true,
		},
		{
			name:   "project covers exact same project",
			owner:  tenancy.Scope{TenantID: "t1", ProjectID: "p1"},
			target: tenancy.Scope{TenantID: "t1", ProjectID: "p1"},
			want:   true,
		},
		{
			name:   "project does not cover tenant",
			owner:  tenancy.Scope{TenantID: "t1", ProjectID: "p1"},
			target: tenancy.Scope{TenantID: "t1"},
			want:   false,
		},
		{
			name:   "project does not cover sibling project",
			owner:  tenancy.Scope{TenantID: "t1", ProjectID: "p1"},
			target: tenancy.Scope{TenantID: "t1", ProjectID: "p2"},
			want:   false,
		},
		{
			name:   "tenant does not cover other tenant",
			owner:  tenancy.Scope{TenantID: "t1"},
			target: tenancy.Scope{TenantID: "t2"},
			want:   false,
		},
		{
			name:   "empty owner tenant rejected",
			owner:  tenancy.Scope{},
			target: tenancy.Scope{TenantID: "t1"},
			want:   false,
		},
		{
			name:   "empty target tenant rejected",
			owner:  tenancy.Scope{TenantID: "t1"},
			target: tenancy.Scope{},
			want:   false,
		},
		{
			name:   "both empty tenants rejected",
			owner:  tenancy.Scope{},
			target: tenancy.Scope{},
			want:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.owner.Contains(tc.target); got != tc.want {
				t.Fatalf("(%+v).Contains(%+v) = %v; want %v", tc.owner, tc.target, got, tc.want)
			}
		})
	}
}
