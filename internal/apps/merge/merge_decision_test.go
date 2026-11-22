package merge_test

import (
	"context"
	"slices"
	"testing"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/merge"
	"github.com/optimiware/kodema/internal/id"
)

func TestParseMergeSubjectSupportsOptionalDecisionID(t *testing.T) {
	fields := append(mergeSubjectFields("shop/web", "refs/heads/main", "base-111", "cand-222"),
		api.SubjectField{Name: merge.MergeFieldDecisionID, Kind: "string", Value: "dec_abc123"},
	)
	parsed, err := merge.ParseMergeSubject(mergeSubject(fields))
	if err != nil {
		t.Fatalf("ParseMergeSubject(with decision_id): %v", err)
	}
	if parsed.DecisionID != id.DecisionID("dec_abc123") {
		t.Fatalf("DecisionID = %q; want dec_abc123", parsed.DecisionID)
	}

	// Invalid decision_id shapes are refused.
	for _, tc := range []struct {
		name  string
		field api.SubjectField
	}{
		{"empty decision_id", api.SubjectField{Name: merge.MergeFieldDecisionID, Kind: "string", Value: ""}},
		{"path traversal decision_id", api.SubjectField{Name: merge.MergeFieldDecisionID, Kind: "string", Value: "../escape"}},
		{"non-string decision_id", api.SubjectField{Name: merge.MergeFieldDecisionID, Kind: "number", Value: "123"}},
		{"unknown extra field", api.SubjectField{Name: "extra", Kind: "string", Value: "x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := append(mergeSubjectFields("shop/web", "refs/heads/main", "base-111", "cand-222"), tc.field)
			if _, err := merge.ParseMergeSubject(mergeSubject(bad)); err == nil {
				t.Fatalf("ParseMergeSubject accepted %s", tc.name)
			}
		})
	}
}

func TestMergeCoordinatorForwardsDecisionIDToGitPushOptions(t *testing.T) {
	var seenPushArgs []string
	exec := &decisionGitExecutor{
		run: func(cmd merge.GitCommand) (merge.GitCommandResult, error) {
			if len(cmd.Args) == 0 {
				return merge.GitCommandResult{}, nil
			}
			switch cmd.Args[0] {
			case "init", "remote", "fetch":
				return merge.GitCommandResult{ExitCode: 0}, nil
			case "rev-parse":
				arg := cmd.Args[len(cmd.Args)-1]
				if arg == "base-111^{commit}" {
					return merge.GitCommandResult{Stdout: []byte("1111111111111111111111111111111111111111\n")}, nil
				}
				return merge.GitCommandResult{Stdout: []byte("2222222222222222222222222222222222222222\n")}, nil
			case "ls-remote":
				if len(seenPushArgs) > 0 {
					return merge.GitCommandResult{Stdout: []byte("2222222222222222222222222222222222222222\trefs/heads/main\n")}, nil
				}
				return merge.GitCommandResult{Stdout: []byte("1111111111111111111111111111111111111111\trefs/heads/main\n")}, nil
			case "merge-base":
				if cmd.Args[2] == "1111111111111111111111111111111111111111" {
					return merge.GitCommandResult{ExitCode: 0}, nil
				}
				return merge.GitCommandResult{ExitCode: 1}, nil
			case "push":
				seenPushArgs = append([]string(nil), cmd.Args...)
				return merge.GitCommandResult{ExitCode: 0}, nil
			default:
				return merge.GitCommandResult{ExitCode: 0}, nil
			}
		},
	}

	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "file:///tmp/remotes",
		StateDirectory: t.TempDir(),
		Executor:       exec,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	obs, err := effects.Execute(context.Background(), merge.MergeOperation{
		Key:         merge.MergeKey{Repository: "shop/web", Ref: "refs/heads/main"},
		OperationID: "op-merge-dec-1",
		Base:        "base-111",
		Candidate:   "cand-222",
		DecisionID:  id.DecisionID("dec_merge_forward_1"),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.State != merge.EffectApplied {
		t.Fatalf("observation state = %q; want applied", obs.State)
	}
	idx := slices.Index(seenPushArgs, "-o")
	if idx < 0 || idx+1 >= len(seenPushArgs) || seenPushArgs[idx+1] != "kodema.decision_id=dec_merge_forward_1" {
		t.Fatalf("git push args = %v; want -o kodema.decision_id=dec_merge_forward_1", seenPushArgs)
	}
}

type decisionGitExecutor struct {
	run func(merge.GitCommand) (merge.GitCommandResult, error)
}

func (s *decisionGitExecutor) Run(_ context.Context, cmd merge.GitCommand) (merge.GitCommandResult, error) {
	return s.run(cmd)
}
