package kv

import (
	"math"
	"testing"

	"github.com/optimiware/kodema/internal/fault"
)

func TestRowInt64Bounds(t *testing.T) {
	valid := []struct {
		name  string
		input any
		want  int64
	}{
		{name: "zero int", input: int(0), want: 0},
		{name: "positive int", input: int(42), want: 42},
		{name: "zero int64", input: int64(0), want: 0},
		{name: "max int64", input: int64(math.MaxInt64), want: math.MaxInt64},
		{name: "zero uint64", input: uint64(0), want: 0},
		{name: "max int64 as uint64", input: uint64(math.MaxInt64), want: math.MaxInt64},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rowInt64(tc.input)
			if err != nil {
				t.Fatalf("rowInt64(%v): %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("rowInt64(%v) = %d; want %d", tc.input, got, tc.want)
			}
		})
	}

	invalid := []struct {
		name  string
		input any
	}{
		{name: "negative int", input: int(-1)},
		{name: "negative int64", input: int64(-1)},
		{name: "uint64 maxInt64+1", input: uint64(math.MaxInt64) + 1},
		{name: "uint64 max", input: uint64(math.MaxUint64)},
		{name: "string", input: "123"},
		{name: "nil", input: nil},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := rowInt64(tc.input); err == nil {
				t.Fatalf("rowInt64(%v) = %d; want internal_failure", tc.input, got)
			} else if class, ok := fault.ClassOf(err); !ok || class != fault.InternalFailure {
				t.Fatalf("rowInt64(%v) class = %v (%v); want internal_failure", tc.input, class, err)
			}
		})
	}
}
