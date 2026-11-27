package cli_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/cli"
	"github.com/optimiware/kodema/internal/apps/cli/userlandfs"
	"github.com/optimiware/kodema/internal/fault"
)

func TestServiceCredentialTTLDefaultAndDayParsing(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantTTL int
	}{
		{
			name:    "default TTL is 24h (86400s)",
			args:    []string{"service", "credential", "issue", "opencode-shop", "--expected-version", "1"},
			wantTTL: 86400,
		},
		{
			name:    "rotate default TTL is 24h (86400s)",
			args:    []string{"service", "credential", "rotate", "opencode-shop", "--expected-version", "1"},
			wantTTL: 86400,
		},
		{
			name:    "7d suffix",
			args:    []string{"service", "credential", "issue", "opencode-shop", "--expected-version", "1", "--ttl", "7d"},
			wantTTL: 7 * 86400,
		},
		{
			name:    "30d suffix",
			args:    []string{"service", "credential", "issue", "opencode-shop", "--expected-version", "1", "--ttl", "30d"},
			wantTTL: 30 * 86400,
		},
		{
			name:    "combined day and hour suffix",
			args:    []string{"service", "credential", "issue", "opencode-shop", "--expected-version", "1", "--ttl", "1d12h"},
			wantTTL: 36 * 3600,
		},
		{
			name:    "integer seconds",
			args:    []string{"service", "credential", "issue", "opencode-shop", "--expected-version", "1", "--ttl", "3600"},
			wantTTL: 3600,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotTTL int
			tok := "kodsvc.opencode-shop.secret123"
			transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
				var req api.ServiceCredentialIssueRequest
				if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
					t.Fatalf("unmarshal req: %v", err)
				}
				gotTTL = req.TTLSeconds
				body, _ := json.Marshal(api.ServiceCredentialIssueResult{State: "current", Version: 2, Token: &tok})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			}}
			cfg := testConfig()
			cfg.Output = cli.OutputTable
			ports, _, errOut := testPorts(transport)
			if code := cli.Run(context.Background(), cfg, tc.args, ports); code != cli.ExitSuccess {
				t.Fatalf("args %v exit=%d stderr=%q", tc.args, code, errOut.String())
			}
			if gotTTL != tc.wantTTL {
				t.Fatalf("args %v ttl_seconds = %d; want %d", tc.args, gotTTL, tc.wantTTL)
			}
		})
	}

	t.Run("invalid TTL formats rejected", func(t *testing.T) {
		for _, badTTL := range []string{"0d", "-7d", "d", "1d1d", "1d-2h", "500ms", "1.5s", "0", "-10", "bogus"} {
			transport := &scriptedTransport{}
			cfg := testConfig()
			ports, _, errOut := testPorts(transport)
			code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "svc", "--expected-version", "1", "--ttl", badTTL}, ports)
			if code != cli.ExitUsage {
				t.Fatalf("--ttl %q exit=%d stderr=%q; want ExitUsage", badTTL, code, errOut.String())
			}
			if len(transport.recorded()) != 0 {
				t.Fatalf("--ttl %q invoked API on invalid input", badTTL)
			}
		}
	})
}

func TestServiceCredentialOutAtomic0600AndNoStdoutLeak(t *testing.T) {
	const secretToken = "kodsvc.opencode-shop.super-secret-plaintext-token"

	for _, subcmd := range []string{"issue", "rotate"} {
		for _, format := range []cli.OutputFormat{cli.OutputTable, cli.OutputJSON} {
			t.Run(subcmd+"_"+string(format)+"_real_os_fs", func(t *testing.T) {
				outPath := filepath.Join(t.TempDir(), "nested", "opencode-shop.token")
				tok := secretToken
				transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
					wantOp := "service.credential." + subcmd
					if inv.Operation != wantOp {
						t.Fatalf("op = %q; want %q", inv.Operation, wantOp)
					}
					body, _ := json.Marshal(api.ServiceCredentialIssueResult{State: "current", Version: 4, Token: &tok})
					return api.Result{Operation: inv.Operation, Body: body}, nil
				}}
				cfg := testConfig()
				cfg.Output = format
				ports, out, errOut := testPorts(transport)
				ports.Files = userlandfs.FileSystem{}

				args := []string{"service", "credential", subcmd, "opencode-shop", "--expected-version", "3", "--ttl", "30d", "--out", outPath}
				if code := cli.Run(context.Background(), cfg, args, ports); code != cli.ExitSuccess {
					t.Fatalf("exit=%d stderr=%q", code, errOut.String())
				}

				// Verify plaintext token NEVER appears in stdout or stderr.
				if strings.Contains(out.String(), secretToken) || strings.Contains(errOut.String(), secretToken) {
					t.Fatalf("plaintext token leaked to output! stdout=%q stderr=%q", out.String(), errOut.String())
				}
				if format == cli.OutputJSON {
					var payload map[string]any
					if err := json.Unmarshal(out.Bytes(), &payload); err != nil {
						t.Fatalf("invalid json stdout %q: %v", out.String(), err)
					}
					if _, hasToken := payload["token"]; hasToken {
						t.Fatalf("json output must omit token field when --out is used: %s", out.String())
					}
					if payload["out"] != outPath {
						t.Fatalf("json out = %v; want %q", payload["out"], outPath)
					}
				} else {
					if strings.Contains(out.String(), "token\t") {
						t.Fatalf("table output must omit token line when --out is used: %q", out.String())
					}
					if !strings.Contains(out.String(), "out\t"+outPath) {
						t.Fatalf("table output missing out path: %q", out.String())
					}
				}

				// Verify OS file permissions (0600) and content.
				info, err := os.Stat(outPath)
				if err != nil {
					t.Fatalf("stat %s: %v", outPath, err)
				}
				if info.Mode().Perm() != 0o600 {
					t.Fatalf("file mode = %04o; want 0600", info.Mode().Perm())
				}
				data, err := os.ReadFile(outPath)
				if err != nil {
					t.Fatalf("read %s: %v", outPath, err)
				}
				if string(data) != secretToken+"\n" {
					t.Fatalf("file content = %q; want %q", string(data), secretToken+"\n")
				}
			})
		}
	}

	t.Run("uses ReplaceFile atomically on fakeFS and preserves existing file if replay omits token", func(t *testing.T) {
		outPath := "/run/kodema/opencode.token"
		fs := newFakeFS()
		fs.files[outPath] = []byte("existing-good-token\n")
		fs.modes[outPath] = 0o600

		// Replayed response omits token -> must fail without overwriting existing file.
		transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
			body, _ := json.Marshal(api.ServiceCredentialIssueResult{State: "current", Version: 4, Token: nil})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		}}
		cfg := testConfig()
		ports, out, errOut := testPorts(transport)
		ports.Files = fs
		if code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "opencode", "--expected-version", "3", "--out", outPath}, ports); code != cli.ExitFailure {
			t.Fatalf("missing token exit=%d stderr=%q; want ExitError", code, errOut.String())
		}
		if out.Len() != 0 {
			t.Fatalf("expected empty stdout on error, got %q", out.String())
		}
		if got := string(fs.files[outPath]); got != "existing-good-token\n" {
			t.Fatalf("existing token file was clobbered: %q", got)
		}
		if len(fs.replaces) != 0 {
			t.Fatalf("ReplaceFile should not have been called when token was absent")
		}
	})

	t.Run("empty --out rejected before API call and nil filesystem rejected before API call", func(t *testing.T) {
		transport := &scriptedTransport{}
		cfg := testConfig()
		ports, _, _ := testPorts(transport)
		if code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "svc", "--expected-version", "1", "--out", "   "}, ports); code != cli.ExitUsage {
			t.Fatalf("empty --out exit=%d; want ExitUsage", code)
		}
		if len(transport.recorded()) != 0 {
			t.Fatal("empty --out called API")
		}

		ports.Files = nil
		if code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "svc", "--expected-version", "1", "--out", "/tmp/tok"}, ports); code != cli.ExitFailure {
			t.Fatalf("nil Files exit=%d; want ExitError", code)
		}
		if len(transport.recorded()) != 0 {
			t.Fatal("nil Files called API and burned one-time token")
		}
	})
}

func TestServiceCredentialRotateIfPresentUpsert(t *testing.T) {
	t.Run("issues directly when no credential exists", func(t *testing.T) {
		tok := "kodsvc.svc-1.fresh-issue"
		var ops []string
		transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
			ops = append(ops, inv.Operation)
			switch inv.Operation {
			case "service.list":
				body, _ := json.Marshal(api.ServiceListResult{
					Services: []api.ServiceSummaryView{{ServiceID: "svc-1", State: "enabled", Version: 2}},
				})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			case "service.credential.issue":
				body, _ := json.Marshal(api.ServiceCredentialIssueResult{State: "current", Version: 3, Token: &tok})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			default:
				t.Fatalf("unexpected op %s", inv.Operation)
				return api.Result{}, nil
			}
		}}
		cfg := testConfig()
		cfg.Output = cli.OutputTable
		ports, out, errOut := testPorts(transport)
		if code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "svc-1", "--rotate-if-present", "--ttl", "7d"}, ports); code != cli.ExitSuccess {
			t.Fatalf("exit=%d stderr=%q", code, errOut.String())
		}
		if len(ops) != 2 || ops[0] != "service.list" || ops[1] != "service.credential.issue" {
			t.Fatalf("ops = %v; want [service.list service.credential.issue]", ops)
		}
		if !strings.Contains(out.String(), tok) {
			t.Fatalf("stdout = %q; want token", out.String())
		}
	})

	t.Run("falls back to rotate when current credential already exists and writes --out", func(t *testing.T) {
		const rotatedTok = "kodsvc.svc-1.rotated-secret"
		tok := rotatedTok
		var ops []string
		var rotateTTL int
		var rotateVer uint64
		transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
			ops = append(ops, inv.Operation)
			switch inv.Operation {
			case "service.list":
				body, _ := json.Marshal(api.ServiceListResult{
					Services: []api.ServiceSummaryView{{ServiceID: "svc-1", State: "enabled", Version: 5}},
				})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			case "service.credential.issue":
				return api.Result{}, fault.New(fault.InvalidInput, "service already has a current credential; rotate it instead")
			case "service.credential.rotate":
				var req api.ServiceCredentialRotateRequest
				_ = json.Unmarshal(inv.Request.Input, &req)
				rotateTTL = req.TTLSeconds
				if inv.Request.ExpectedVersion != nil {
					rotateVer = *inv.Request.ExpectedVersion
				}
				body, _ := json.Marshal(api.ServiceCredentialRotateResult{State: "current", Version: 6, Token: &tok})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			default:
				t.Fatalf("unexpected op %s", inv.Operation)
				return api.Result{}, nil
			}
		}}
		outPath := filepath.Join(t.TempDir(), "svc-1.token")
		cfg := testConfig()
		cfg.Output = cli.OutputTable
		ports, out, errOut := testPorts(transport)
		ports.Files = userlandfs.FileSystem{}

		args := []string{"service", "credential", "issue", "svc-1", "--ttl", "30d", "--rotate-if-present", "--out", outPath}
		if code := cli.Run(context.Background(), cfg, args, ports); code != cli.ExitSuccess {
			t.Fatalf("exit=%d stderr=%q", code, errOut.String())
		}
		if len(ops) != 3 || ops[0] != "service.list" || ops[1] != "service.credential.issue" || ops[2] != "service.credential.rotate" {
			t.Fatalf("ops = %v; want [service.list service.credential.issue service.credential.rotate]", ops)
		}
		if rotateTTL != 30*86400 || rotateVer != 5 {
			t.Fatalf("rotate ttl=%d ver=%d; want ttl=%d ver=5", rotateTTL, rotateVer, 30*86400)
		}
		if strings.Contains(out.String()+errOut.String(), rotatedTok) {
			t.Fatalf("plaintext token leaked on --rotate-if-present --out: stdout=%q stderr=%q", out.String(), errOut.String())
		}
		data, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("read %s: %v", outPath, err)
		}
		if string(data) != rotatedTok+"\n" {
			t.Fatalf("file content = %q; want %q", string(data), rotatedTok+"\n")
		}
		info, _ := os.Stat(outPath)
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("file mode = %04o; want 0600", info.Mode().Perm())
		}
	})

	t.Run("does NOT fall back to rotate on unrelated InvalidInput or Forbidden", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{name: "ttl bounds error", err: fault.New(fault.InvalidInput, "service credential ttl is outside the allowed bounds [15s, 720h0m0s]")},
			{name: "forbidden", err: fault.New(fault.Forbidden, "service is disabled")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var ops []string
				transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
					ops = append(ops, inv.Operation)
					return api.Result{}, tc.err
				}}
				cfg := testConfig()
				ports, _, errOut := testPorts(transport)
				code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "svc-1", "--expected-version", "2", "--rotate-if-present"}, ports)
				if code != cli.ExitFailure {
					t.Fatalf("exit=%d; want ExitError", code)
				}
				if len(ops) != 1 || ops[0] != "service.credential.issue" {
					t.Fatalf("ops = %v; must not fall back to rotate", ops)
				}
				if !strings.Contains(errOut.String(), tc.err.Error()) {
					t.Fatalf("stderr = %q; want original error %q", errOut.String(), tc.err.Error())
				}
			})
		}
	})

	t.Run("rejects --rotate-if-present on rotate subcommand", func(t *testing.T) {
		transport := &scriptedTransport{}
		cfg := testConfig()
		ports, _, errOut := testPorts(transport)
		code := cli.Run(context.Background(), cfg, []string{"service", "credential", "rotate", "svc-1", "--expected-version", "1", "--rotate-if-present"}, ports)
		if code != cli.ExitUsage {
			t.Fatalf("exit=%d stderr=%q; want ExitUsage", code, errOut.String())
		}
	})
}

func TestServiceCredentialTenantWideLookupAndMutualExclusivity(t *testing.T) {
	tok := "kodsvc.git.tenant-secret"
	var sawTenantList bool
	transport := &scriptedTransport{respond: func(_ context.Context, _ int, inv api.Invocation) (api.Result, error) {
		switch inv.Operation {
		case "service.list":
			var req api.ServiceListRequest
			_ = json.Unmarshal(inv.Request.Input, &req)
			if req.ProjectID != "" {
				t.Fatalf("tenant-wide service.list sent project_id=%q; want empty", req.ProjectID)
			}
			sawTenantList = true
			body, _ := json.Marshal(api.ServiceListResult{
				Services: []api.ServiceSummaryView{{ServiceID: "git", State: "enabled", Version: 7}},
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case "service.credential.issue":
			if inv.Request.ExpectedVersion == nil || *inv.Request.ExpectedVersion != 7 {
				t.Fatalf("expected_version = %v; want 7", inv.Request.ExpectedVersion)
			}
			body, _ := json.Marshal(api.ServiceCredentialIssueResult{State: "current", Version: 8, Token: &tok})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case "service.credential.revoke":
			invFlag := true
			body, _ := json.Marshal(api.ServiceCredentialRevokeResult{Version: 8, Invalidated: &invFlag})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		default:
			t.Fatalf("unexpected op %s", inv.Operation)
			return api.Result{}, nil
		}
	}}

	cfg := testConfig() // has Project: "shop"
	cfg.Output = cli.OutputTable
	ports, _, errOut := testPorts(transport)
	if code := cli.Run(context.Background(), cfg, []string{"service", "credential", "issue", "git", "--tenant-wide", "--ttl", "30d"}, ports); code != cli.ExitSuccess {
		t.Fatalf("tenant-wide issue exit=%d stderr=%q", code, errOut.String())
	}
	if !sawTenantList {
		t.Fatal("expected tenant-wide service.list lookup")
	}

	ports, _, errOut = testPorts(transport)
	if code := cli.Run(context.Background(), cfg, []string{"service", "credential", "revoke", "git", "--tenant-wide"}, ports); code != cli.ExitSuccess {
		t.Fatalf("tenant-wide revoke exit=%d stderr=%q", code, errOut.String())
	}

	for _, badArgs := range [][]string{
		{"service", "credential", "issue", "git", "--tenant-wide", "--project", "shop"},
		{"service", "credential", "rotate", "git", "--tenant-wide", "-p", "shop"},
		{"service", "credential", "revoke", "git", "--tenant-wide", "--project", "shop"},
	} {
		ports, _, errOut = testPorts(transport)
		if code := cli.Run(context.Background(), cfg, badArgs, ports); code != cli.ExitUsage {
			t.Fatalf("args %v exit=%d stderr=%q; want ExitUsage", badArgs, code, errOut.String())
		}
	}
}
