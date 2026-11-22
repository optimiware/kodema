//go:build component

package kubernetesconformance

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
	runtimesupport "github.com/optimiware/kodema/test/support/infrastructure/runtime"
)

// Load the checkout's real image into this test's disposable kind node. The
// Job uses its manifest digest, just as a registry-backed installation does.
func loadToolbox(t *testing.T, ctx context.Context, fx *fixture) string {
	t.Helper()
	run := func(name string, args ...string) string {
		t.Helper()
		data, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, data)
		}
		return strings.TrimSpace(string(data))
	}
	const repo = "localhost/kodema-kind-test"
	tag := repo + ":" + fx.Cluster.Name
	run("../../../scripts/image", "build", "--repository", repo, "--tag", fx.Cluster.Name)
	t.Cleanup(func() { _ = exec.Command("podman", "image", "rm", tag).Run() })
	archive := filepath.Join(t.TempDir(), "toolbox.tar")
	run("podman", "save", "--format", "oci-archive", "-o", archive, tag)
	// OCI export may recompress layers. Use the manifest digest that enters
	// containerd, not Podman's pre-export store digest.
	var index struct{ Manifests []struct{ Digest string } }
	if err := json.Unmarshal([]byte(run("tar", "-xOf", archive, "index.json")), &index); err != nil || len(index.Manifests) != 1 {
		t.Fatalf("expected one exported toolbox manifest: %v", err)
	}
	pinned := repo + "@" + index.Manifests[0].Digest
	file, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	importImage := exec.CommandContext(ctx, "podman", "exec", "-i", fx.Cluster.Name+"-control-plane", "ctr", "-n", "k8s.io", "images", "import", "--digests", "-")
	importImage.Stdin = file
	if out, err := importImage.CombinedOutput(); err != nil {
		t.Fatalf("load toolbox into kind: %v\n%s", err, out)
	}
	run("podman", "exec", fx.Cluster.Name+"-control-plane", "ctr", "-n", "k8s.io", "images", "tag", "--force", tag, pinned)
	return pinned
}

func TestJobToolboxGitAndFileOutput(t *testing.T) {
	t.Parallel()
	const ws = id.WorkspaceID("workspace-toolbox")
	fx, ctx := newFixture(t, ws, fixtureOptions{withClaim: true})
	image := loadToolbox(t, ctx, fx)
	putWorkloadCASecret(t, ctx, fx, workloadCASecret, clusterTrustCA(t, ctx, fx))
	adapter, err := runtimekube.New(runtimekube.Config{
		Cluster: fx.Client, Namespace: fx.Namespace, TenantID: fx.Tenant,
		KodemaURL: "http://127.0.0.1:18081", ToolboxImage: image, GitURL: "https://git.example.test",
		WorkloadCA: runtimekube.WorkloadCAConfig{SecretName: workloadCASecret},
		Policy:     runtimekube.Policy{CPURequest: "50m", CPULimit: "500m", MemoryRequest: "32Mi", MemoryLimit: "256Mi", JobTTLSeconds: 120},
		Cleanup:    runtimekube.CleanupPolicy{Attempts: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.Adapter = adapter
	// This ordinary image provides Git and Python, not Kodema. The loopback
	// fixture accepts exactly the authenticated output written by the script.
	identity := testIdentity("exec-toolbox", "act-build", "workload-toolbox", "toolbox")
	request := runtimesupport.StandardPrepareRequest(identity, ws,
		"docker.io/library/buildpack-deps@sha256:712f969c9d81a35f9f00cce701faae521891d42a20b592b61bd0e6c5328fad7c",
		[]string{
			`test "$PWD" = /workspace && test "$(command -v kodema)" = /kodema/bin/kodema`,
			`kodema version`,
			`test ! -e /kodema/toolbox-plan.json && test -s "$KODEMA_CA_FILE"`,
			`if touch /kodema/bin/writable 2>/dev/null; then exit 1; fi`,
			`printf 'url=%s/repo\n\n' "$KODEMA_GIT_URL" | GIT_TERMINAL_PROMPT=0 git credential fill >/dev/null`,
			`cat /kodema/outputs/earlier/out-a > /kodema/outputs/build/result`,
			`python3 /kodema/inputs/server`,
		})
	request.TaskID = ""
	request.Container.Env = nil
	request.Container.Secrets = nil
	request.Container.Outputs = []id.OutputKey{"result"}
	request.Secrets = nil
	request.Credential = "test-execution-token"
	request.Inputs["server"] = `import http.server, json, os
class API(http.server.BaseHTTPRequestHandler):
    def reply(self, op, body):
        assert self.headers["Authorization"] == "Bearer test-execution-token"
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({"operation":op, "body":body}).encode())
    def do_GET(self):
        assert self.path == "/api/v1/executions/exec-toolbox"
        self.reply("execution.inspect", {"version":4,"actions":[{"action_id":"act-build","name":"build"}]})
    def do_PUT(self):
        assert self.path == "/api/v1/executions/exec-toolbox/actions/act-build/outputs"
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert req["expected_version"] == 4 and req["input"]["outputs"] == {"result":"earlier-output"}
        self.reply("execution.action.outputs", {"execution_version":5,"action_version":2})
server = http.server.HTTPServer(("127.0.0.1",18081), API)
if os.fork() == 0:
    server.serve_forever()
`
	startPrepared(t, ctx, fx, request)
	// Ask Kubernetes for a bounded wait and pod events on failure, including
	// init-container failures that cannot be reported as workload exits.
	job := oneJob(t, ctx, fx.Client, identity)
	wait, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	_, waitErr := fx.Cluster.Kubectl(wait, "wait", "-n", fx.Namespace, "--for=condition=complete", "job/"+job.Name, "--timeout=170s")
	if waitErr != nil {
		events, _ := fx.Cluster.Kubectl(ctx, "get", "events", "-n", fx.Namespace, "--sort-by=.metadata.creationTimestamp")
		logs, _ := fx.Cluster.Kubectl(ctx, "logs", "-n", fx.Namespace, "job/"+job.Name, "-c", "workload")
		t.Fatalf("toolbox Job: %v\n%s\n%s", waitErr, events.Stdout, logs.Stdout)
	}
	observed := waitForObservation(t, ctx, adapter, reconcile.ObserveRequest{Identity: identity, WorkspaceID: ws}, engine.ObservationExited)
	stdout, stderr := runtimesupport.ReadAllLogs(t, ctx, adapter, identity, 1<<20)
	if code, ok := observed.ExitStatus(); !ok || code != 0 || !strings.Contains(stderr, "submitted outputs result") {
		t.Fatalf("workload exit=%d/%t stdout=%q stderr=%q", code, ok, stdout, stderr)
	}
	if strings.Contains(stdout, "submitted outputs result") {
		t.Fatalf("stderr output leaked into stdout: stdout=%q stderr=%q", stdout, stderr)
	}
}

// TestJobToolboxSplitsStdoutAndStderr proves that when a Kubernetes adapter
// mounts the toolbox image, the stateless stream-framing wrapper separates
// workload stdout and stderr cleanly across interleaved lines, partial chunks,
// and secret redaction on a real kind cluster.
func TestJobToolboxSplitsStdoutAndStderr(t *testing.T) {
	t.Parallel()
	const ws = id.WorkspaceID("workspace-toolbox-split")
	fx, ctx := newFixture(t, ws, fixtureOptions{withClaim: true})
	image := loadToolbox(t, ctx, fx)
	adapter, err := runtimekube.New(runtimekube.Config{
		Cluster:      fx.Client,
		Namespace:    fx.Namespace,
		TenantID:     fx.Tenant,
		KodemaURL:    testKodemaURL,
		ToolboxImage: image,
		Policy:       runtimekube.Policy{CPURequest: "50m", CPULimit: "500m", MemoryRequest: "32Mi", MemoryLimit: "256Mi", JobTTLSeconds: 120},
		Cleanup:      runtimekube.CleanupPolicy{Attempts: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	fx.Adapter = adapter

	identity := testIdentity("exec-split-logs", "act-split-logs", "workload-split-logs", "split-logs")
	request := workloadRequest(identity, ws, []string{
		`printf 'out-line-1\n'`,
		`printf 'err-line-1\n' >&2`,
		`printf 'out-secret:%s\n' "$(cat /kodema/secrets/KODEMA_TOKEN)"`,
		`printf 'err-secret:%s\n' "$(cat /kodema/secrets/KODEMA_TOKEN)" >&2`,
		`printf 'out-partial'`,
		`printf 'err-partial' >&2`,
	}, "tok-split-secret")
	startPrepared(t, ctx, fx, request)

	observed := waitForObservation(t, ctx, adapter, reconcile.ObserveRequest{Identity: identity, WorkspaceID: ws}, engine.ObservationExited)
	if code, ok := observed.ExitStatus(); !ok || code != 0 {
		t.Fatalf("split-logs exit = (%d, %v); want (0, true)", code, ok)
	}
	stdout, stderr := runtimesupport.ReadAllLogs(t, ctx, adapter, identity, 13)
	wantStdout := "out-line-1\nout-secret:" + runtimekube.Redacted + "\nout-partial"
	wantStderr := "err-line-1\nerr-secret:" + runtimekube.Redacted + "\nerr-partial"
	if stdout != wantStdout {
		t.Fatalf("stdout = %q; want %q", stdout, wantStdout)
	}
	if stderr != wantStderr {
		t.Fatalf("stderr = %q; want %q", stderr, wantStderr)
	}
	if strings.Contains(stdout, "tok-split-secret") || strings.Contains(stderr, "tok-split-secret") {
		t.Fatal("credential leaked into split stdout/stderr logs")
	}
}
