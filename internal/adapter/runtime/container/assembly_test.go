package container

import (
	"slices"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

const testPinnedImage = "registry.example.test/kodema/runner@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func samplePrepareRequest() reconcile.PrepareRequest {
	return reconcile.PrepareRequest{
		Identity:    validIdentity(),
		WorkspaceID: "ws-1",
		ActionName:  "build",
		TaskID:      "task-1",
		Container: definition.Container{
			Image:    testPinnedImage,
			Commands: []string{"echo hi"},
			Env: []definition.EnvVar{
				{Name: "APP_ENV", Value: "test"},
				{Name: "KODEMA_LLM_OPENAI_BASE_URL", Value: "https://kodema.local/llm/openai/v1"},
			},
			Outputs: []id.OutputKey{"artifact"},
			Resources: &definition.Resources{
				Memory: "1Gi",
				CPU:    "1",
			},
		},
		Caches: []reconcile.CacheMount{{Name: "go", Key: "0123456789abcdef0123456789abcdef"}},
		Secrets: []reconcile.SecretValue{
			{Name: "Z_SECRET", Value: "z-val"},
			{Name: "A_SECRET", Value: "a-val"},
		},
		Inputs: map[id.InputKey]string{
			"b_in": "two",
			"a_in": "one",
		},
		Outputs: map[id.ActionName]engine.Outputs{
			"prev": {"out_b": "2", "out_a": "1"},
		},
		Task:       []byte(`[{"role":"user","content":"hi"}]`),
		TaskFiles:  map[string][]byte{"title": []byte("My task")},
		Credential: "initial-token",
	}
}

func TestBuildMaterialization(t *testing.T) {
	req := samplePrepareRequest()
	mat, err := BuildMaterialization(req, 0)
	if err != nil {
		t.Fatalf("BuildMaterialization = %v; want nil", err)
	}
	if mat.WorkspaceID != req.WorkspaceID {
		t.Fatalf("WorkspaceID = %q; want %q", mat.WorkspaceID, req.WorkspaceID)
	}
	if mat.Credential != req.Credential {
		t.Fatalf("Credential = %q; want %q", mat.Credential, req.Credential)
	}
	if len(mat.Secrets) != 2 {
		t.Fatalf("Secrets len = %d; want 2", len(mat.Secrets))
	}

	gotPaths := make([]string, 0, len(mat.Files))
	for _, f := range mat.Files {
		if f.Mode != ReadOnlyFileMode {
			t.Errorf("file %q mode = %#o; want %#o", f.Path, f.Mode, ReadOnlyFileMode)
		}
		gotPaths = append(gotPaths, f.Path)
	}
	wantPaths := []string{
		"/kodema/inputs/a_in",
		"/kodema/inputs/b_in",
		"/kodema/outputs/prev/out_a",
		"/kodema/outputs/prev/out_b",
		"/kodema/task.json",
		"/kodema/task/title",
	}
	if !slices.Equal(gotPaths, wantPaths) {
		t.Fatalf("materialized paths = %v; want %v", gotPaths, wantPaths)
	}

	// Custom mode is preserved when non-zero.
	matCustom, err := BuildMaterialization(req, 0o400)
	if err != nil {
		t.Fatalf("BuildMaterialization(0400) = %v; want nil", err)
	}
	if matCustom.Files[0].Mode != 0o400 {
		t.Fatalf("custom mode = %#o; want 0400", matCustom.Files[0].Mode)
	}

	// Rejects invalid workspace id, invalid secret name, invalid input key, empty credential.
	badWS := samplePrepareRequest()
	badWS.WorkspaceID = "bad ws"
	_, err = BuildMaterialization(badWS, 0)
	assertFaultClass(t, err, fault.InvalidInput)

	badSecret := samplePrepareRequest()
	badSecret.Secrets = []reconcile.SecretValue{{Name: id.SecretTokenName, Value: "x"}}
	_, err = BuildMaterialization(badSecret, 0)
	assertFaultClass(t, err, fault.InvalidInput)

	badCred := samplePrepareRequest()
	badCred.Credential = ""
	_, err = BuildMaterialization(badCred, 0)
	assertFaultClass(t, err, fault.InvalidInput)
}

func TestPreparationDigest(t *testing.T) {
	req := samplePrepareRequest()
	mat, err := BuildMaterialization(req, 0)
	if err != nil {
		t.Fatalf("BuildMaterialization = %v", err)
	}

	base := PreparationDigest(req, mat, "https://kodema.local")
	if len(base) != 48 {
		t.Fatalf("digest len = %d; want 48", len(base))
	}

	// Mutable credential rotation MUST NOT change the digest.
	rotated := samplePrepareRequest()
	rotated.Credential = "rotated-token-2"
	matRotated, err := BuildMaterialization(rotated, 0)
	if err != nil {
		t.Fatalf("BuildMaterialization(rotated) = %v", err)
	}
	if got := PreparationDigest(rotated, matRotated, "https://kodema.local"); got != base {
		t.Fatalf("credential rotation changed digest: %q != %q", got, base)
	}

	// Secret order MUST NOT change the digest.
	reordered := samplePrepareRequest()
	reordered.Secrets = []reconcile.SecretValue{
		{Name: "A_SECRET", Value: "a-val"},
		{Name: "Z_SECRET", Value: "z-val"},
	}
	matReordered, err := BuildMaterialization(reordered, 0)
	if err != nil {
		t.Fatalf("BuildMaterialization(reordered) = %v", err)
	}
	if got := PreparationDigest(reordered, matReordered, "https://kodema.local"); got != base {
		t.Fatalf("secret ordering changed digest: %q != %q", got, base)
	}

	// Every meaningful activation change MUST change the digest.
	mutations := []struct {
		name      string
		kodemaURL string
		mutate    func(*reconcile.PrepareRequest)
	}{
		{"execution id", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Identity.ExecutionID = "exec-2" }},
		{"idempotency key", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Identity.IdempotencyKey = "idem-2" }},
		{"action name", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.ActionName = "test" }},
		{"task id", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.TaskID = "task-2" }},
		{"image", "https://kodema.local", func(r *reconcile.PrepareRequest) {
			r.Container.Image = "registry.example.test/kodema/other@sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
		}},
		{"commands", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Container.Commands = []string{"echo bye"} }},
		{"env", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Container.Env[0].Value = "prod" }},
		{"declared outputs", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Container.Outputs = []id.OutputKey{"other"} }},
		{"resources", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Container.Resources.Memory = "2Gi" }},
		{"caches", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Caches[0].Name = "npm" }},
		{"kodema url", "https://kodema-2.local", func(r *reconcile.PrepareRequest) {}},
		{"input content", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Inputs["a_in"] = "changed" }},
		{"secret value", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Secrets[0].Value = "changed" }},
		{"task absent", "https://kodema.local", func(r *reconcile.PrepareRequest) { r.Task = nil; r.TaskFiles = nil }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			mut := samplePrepareRequest()
			tc.mutate(&mut)
			m, err := BuildMaterialization(mut, 0)
			if err != nil {
				t.Fatalf("BuildMaterialization = %v", err)
			}
			if got := PreparationDigest(mut, m, tc.kodemaURL); got == base {
				t.Fatalf("mutation %q produced identical digest %q", tc.name, got)
			}
		})
	}
}

func TestBuildContainerEnvAndEnvStrings(t *testing.T) {
	admitted := []definition.EnvVar{
		{Name: "FIRST", Value: "1"},
		{Name: "KODEMA_LLM_OPENAI_BASE_URL", Value: "https://kodema.local/llm/openai/v1"},
		{Name: "KODEMA_LLM_CUSTOM_BASE_URL", Value: "https://kodema.local/llm/custom/v1"},
		{Name: id.EnvName(id.EnvURL), Value: "https://spoof.invalid"},
		{Name: id.EnvName(id.EnvExecution), Value: "spoof-exec"},
		{Name: id.EnvName(id.EnvAction), Value: "spoof-action"},
		{Name: id.EnvName(id.EnvTask), Value: "spoof-task"},
		{Name: id.EnvName(workspace.EnvCAFile), Value: "/spoof/ca.crt"},
		{Name: id.EnvName(workspace.EnvGitURL), Value: "https://spoof-git.invalid"},
		{Name: "GIT_CONFIG_COUNT", Value: "99"},
		{Name: "GIT_CONFIG_KEY_0", Value: "credential.helper"},
		{Name: "GIT_CONFIG_VALUE_0", Value: "store"},
		{Name: "SECOND", Value: "2"},
	}

	vars, err := BuildContainerEnv(admitted, EnvOptions{
		KodemaURL:   "https://kodema.local",
		ExecutionID: "exec-1",
		ActionName:  "build",
		TaskID:      "task-1",
		CAFile:      WorkloadCAPath,
		GitURL:      "https://git.kodema.local/tenant-a/",
		Toolbox:     true,
	})
	if err != nil {
		t.Fatalf("BuildContainerEnv = %v; want nil", err)
	}

	// Verify order: admitted non-stripped first, then reserved context, then CA, then Git.
	wantPrefix := []EnvVar{
		{Name: "FIRST", Value: "1"},
		{Name: "KODEMA_LLM_OPENAI_BASE_URL", Value: "https://kodema.local/llm/openai/v1"},
		{Name: "KODEMA_LLM_CUSTOM_BASE_URL", Value: "https://kodema.local/llm/custom/v1"},
		{Name: "SECOND", Value: "2"},
		{Name: id.EnvURL, Value: "https://kodema.local"},
		{Name: id.EnvExecution, Value: "exec-1"},
		{Name: id.EnvAction, Value: "build"},
		{Name: id.EnvTask, Value: "task-1"},
		{Name: workspace.EnvCAFile, Value: WorkloadCAPath},
		{Name: workspace.EnvGitURL, Value: "https://git.kodema.local/tenant-a"},
		{Name: "GIT_CONFIG_COUNT", Value: "4"},
	}
	if len(vars) < len(wantPrefix) {
		t.Fatalf("len(vars) = %d; want at least %d (%+v)", len(vars), len(wantPrefix), vars)
	}
	if !slices.Equal(vars[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("vars prefix =\n%#v\nwant\n%#v", vars[:len(wantPrefix)], wantPrefix)
	}

	strs := EnvStrings(vars)
	if len(strs) != len(vars) {
		t.Fatalf("len(EnvStrings) = %d; want %d", len(strs), len(vars))
	}
	if strs[0] != "FIRST=1" || !slices.Contains(strs, workspace.EnvCAFile+"="+WorkloadCAPath) {
		t.Fatalf("unexpected EnvStrings: %v", strs)
	}

	// Task-free without CA or Git preserves user KODEMA_CA_FILE and GIT_CONFIG_* and omits KODEMA_TASK.
	plain, err := BuildContainerEnv([]definition.EnvVar{
		{Name: id.EnvName(workspace.EnvCAFile), Value: "/custom/ca.pem"},
		{Name: "GIT_CONFIG_COUNT", Value: "1"},
	}, EnvOptions{
		KodemaURL:   "https://kodema.local",
		ExecutionID: "exec-1",
		ActionName:  "build",
	})
	if err != nil {
		t.Fatalf("BuildContainerEnv(plain) = %v; want nil", err)
	}
	for _, entry := range plain {
		if entry.Name == id.EnvTask {
			t.Fatalf("task-free env unexpectedly included %s", id.EnvTask)
		}
	}
	if plain[0].Name != workspace.EnvCAFile || plain[0].Value != "/custom/ca.pem" {
		t.Fatalf("expected user CA file preserved when opts.CAFile is empty, got %+v", plain)
	}
	if plain[1].Name != "GIT_CONFIG_COUNT" {
		t.Fatalf("expected user GIT_CONFIG_COUNT preserved when opts.GitURL is empty, got %+v", plain)
	}

	// Invalid GitURL fails cleanly.
	_, err = BuildContainerEnv(nil, EnvOptions{
		KodemaURL:   "https://kodema.local",
		ExecutionID: "exec-1",
		ActionName:  "build",
		GitURL:      "http://insecure.invalid",
	})
	assertFaultClass(t, err, fault.InvalidInput)
}

func TestValidateContainerEnv(t *testing.T) {
	if err := ValidateContainerEnv([]definition.EnvVar{
		{Name: "FOO", Value: "bar"},
		{Name: "KODEMA_LLM_OPENAI_BASE_URL", Value: "https://kodema.local/llm/openai/v1"},
	}); err != nil {
		t.Fatalf("ValidateContainerEnv(valid) = %v; want nil", err)
	}
	assertFaultClass(t, ValidateContainerEnv([]definition.EnvVar{{Name: id.EnvName(id.EnvURL), Value: "x"}}), fault.InvalidInput)
	assertFaultClass(t, ValidateContainerEnv([]definition.EnvVar{{Name: "123BAD", Value: "x"}}), fault.InvalidInput)
}

func TestWorkloadRole(t *testing.T) {
	sandboxReq := func(cmds []string, declared []id.SecretName, resolved []reconcile.SecretValue) reconcile.PrepareRequest {
		return reconcile.PrepareRequest{
			ActionName: RoleSandbox,
			Container: definition.Container{
				Commands: cmds,
				Secrets:  declared,
			},
			Secrets: resolved,
		}
	}

	if got := WorkloadRole(sandboxReq(
		[]string{"# comment\n  exec /kodema/bin/kodema sandbox serve --port 4096"},
		[]id.SecretName{"KODEMA_SBOX_TOK_1"},
		[]reconcile.SecretValue{{Name: "KODEMA_SBOX_TOK_1", Value: "tok"}},
	)); got != RoleSandbox {
		t.Fatalf("WorkloadRole(valid exec sandbox) = %q; want %q", got, RoleSandbox)
	}

	if got := WorkloadRole(sandboxReq(
		[]string{"/kodema/bin/kodema sandbox serve"},
		nil,
		nil,
	)); got != RoleSandbox {
		t.Fatalf("WorkloadRole(valid plain sandbox) = %q; want %q", got, RoleSandbox)
	}

	// Non-sandbox action name.
	nonSandbox := sandboxReq([]string{"/kodema/bin/kodema sandbox serve"}, nil, nil)
	nonSandbox.ActionName = "build"
	if got := WorkloadRole(nonSandbox); got != "" {
		t.Fatalf("WorkloadRole(wrong action name) = %q; want empty", got)
	}

	// Non-sandbox declared secret disqualifies role.
	if got := WorkloadRole(sandboxReq(
		[]string{"/kodema/bin/kodema sandbox serve"},
		[]id.SecretName{"PROD_DB_PASSWORD"},
		nil,
	)); got != "" {
		t.Fatalf("WorkloadRole(non-sandbox declared secret) = %q; want empty", got)
	}

	// Non-sandbox resolved secret disqualifies role.
	if got := WorkloadRole(sandboxReq(
		[]string{"/kodema/bin/kodema sandbox serve"},
		nil,
		[]reconcile.SecretValue{{Name: "API_KEY", Value: "v"}},
	)); got != "" {
		t.Fatalf("WorkloadRole(non-sandbox resolved secret) = %q; want empty", got)
	}

	// Command without sandbox serve disqualifies role.
	if got := WorkloadRole(sandboxReq(
		[]string{"# /kodema/bin/kodema sandbox serve\necho hi"},
		nil,
		nil,
	)); got != "" {
		t.Fatalf("WorkloadRole(commented sandbox serve) = %q; want empty", got)
	}
}

func TestValidatePinnedImage(t *testing.T) {
	if err := ValidatePinnedImage(testPinnedImage); err != nil {
		t.Fatalf("ValidatePinnedImage(valid) = %v; want nil", err)
	}
	if err := ValidatePinnedImage("alpine:3.20@sha256:" + strings.Repeat("a", 64)); err != nil {
		t.Fatalf("ValidatePinnedImage(tag+digest) = %v; want nil", err)
	}

	for _, bad := range []string{
		"",
		"   ",
		"alpine:3.20",
		"alpine@sha256:1234",
		"alpine@md5:" + strings.Repeat("a", 32),
		"@sha256:" + strings.Repeat("a", 64),
		"bad repo@sha256:" + strings.Repeat("a", 64),
		"alpine@sha256:" + strings.Repeat("g", 64),
	} {
		t.Run(bad, func(t *testing.T) {
			assertFaultClass(t, ValidatePinnedImage(bad), fault.InvalidInput)
		})
	}
}
