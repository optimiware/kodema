package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/control/token"
	"github.com/optimiware/kodema/internal/fault"
)

// TestServerServiceCredentialMaxTTLEndToEnd proves that Config.ServiceCredentialMaxTTL
// defaults to 30 days, accepts custom overrides through wireApp, rejects
// invalid bounds at startup, and leaves execution credentials capped at 8h.
func TestServerServiceCredentialMaxTTLEndToEnd(t *testing.T) {
	t.Run("default allows 30d and rejects >30d with concrete bounds", func(t *testing.T) {
		environment := newTestEnvironment(t)
		app := environment.startApp(t)
		handler := app.Handler()
		projectToken := "dev-project-token"

		recorder, result, publicError := restCall(t, handler, http.MethodPost,
			"/api/v1/projects/project-1/services", projectToken,
			api.ServiceCreateRequest{ServiceID: "agent-svc", Roles: []string{}}, nil, "svc-create-default")
		if recorder.Code != api.HTTPStatusSuccess {
			t.Fatalf("service.create status=%d error=%+v", recorder.Code, publicError)
		}
		created := decodeBody[api.ServiceCreateResult](t, result)
		v := created.Version

		// 30d succeeds.
		recorder, result, publicError = restCall(t, handler, http.MethodPost,
			"/api/v1/services/agent-svc/credentials", projectToken,
			api.ServiceCredentialIssueRequest{TTLSeconds: int((30 * 24 * time.Hour) / time.Second)}, &v, "cred-issue-30d")
		if recorder.Code != api.HTTPStatusSuccess {
			t.Fatalf("service.credential.issue 30d status=%d error=%+v", recorder.Code, publicError)
		}
		issued := decodeBody[api.ServiceCredentialIssueResult](t, result)
		if issued.Token == nil || !strings.HasPrefix(*issued.Token, token.ServicePrefix) {
			t.Fatalf("service.credential.issue 30d returned unexpected token: %+v", issued)
		}

		// 30d + 1s fails with concrete bounds.
		v2 := issued.Version
		recorder, _, publicError = restCall(t, handler, http.MethodPost,
			"/api/v1/services/agent-svc/credentials/rotate", projectToken,
			api.ServiceCredentialRotateRequest{TTLSeconds: int((30*24*time.Hour)/time.Second) + 1}, &v2, "cred-rotate-over")
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("service.credential.rotate >30d status=%d; want 400", recorder.Code)
		}
		if publicError.Code != string(fault.InvalidInput) || !strings.Contains(publicError.Message, "[15s, 720h0m0s]") {
			t.Fatalf("service.credential.rotate >30d error = %+v; want invalid_input with [15s, 720h0m0s]", publicError)
		}
	})

	t.Run("custom ServiceCredentialMaxTTL override is enforced", func(t *testing.T) {
		environment := newTestEnvironment(t)
		environment.config.ServiceCredentialMaxTTL = 48 * time.Hour
		app := environment.startApp(t)
		handler := app.Handler()
		projectToken := "dev-project-token"

		recorder, result, publicError := restCall(t, handler, http.MethodPost,
			"/api/v1/projects/project-1/services", projectToken,
			api.ServiceCreateRequest{ServiceID: "custom-svc", Roles: []string{}}, nil, "svc-create-custom")
		if recorder.Code != api.HTTPStatusSuccess {
			t.Fatalf("service.create status=%d error=%+v", recorder.Code, publicError)
		}
		created := decodeBody[api.ServiceCreateResult](t, result)
		v := created.Version

		// 48h succeeds.
		recorder, result, publicError = restCall(t, handler, http.MethodPost,
			"/api/v1/services/custom-svc/credentials", projectToken,
			api.ServiceCredentialIssueRequest{TTLSeconds: int((48 * time.Hour) / time.Second)}, &v, "cred-issue-48h")
		if recorder.Code != api.HTTPStatusSuccess {
			t.Fatalf("service.credential.issue 48h status=%d error=%+v", recorder.Code, publicError)
		}
		issued := decodeBody[api.ServiceCredentialIssueResult](t, result)

		// 48h + 1s fails with custom concrete bounds.
		v2 := issued.Version
		recorder, _, publicError = restCall(t, handler, http.MethodPost,
			"/api/v1/services/custom-svc/credentials/rotate", projectToken,
			api.ServiceCredentialRotateRequest{TTLSeconds: int((48*time.Hour)/time.Second) + 1}, &v2, "cred-rotate-over-48h")
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("service.credential.rotate >48h status=%d; want 400", recorder.Code)
		}
		if publicError.Code != string(fault.InvalidInput) || !strings.Contains(publicError.Message, "[15s, 48h0m0s]") {
			t.Fatalf("service.credential.rotate >48h error = %+v; want invalid_input with [15s, 48h0m0s]", publicError)
		}
	})

	t.Run("invalid ServiceCredentialMaxTTL is refused at startup", func(t *testing.T) {
		for _, bad := range []time.Duration{-time.Hour, 5 * time.Second} {
			environment := newTestEnvironment(t)
			environment.config.ServiceCredentialMaxTTL = bad
			if _, err := New(context.Background(), environment.config); err == nil {
				t.Fatalf("New accepted invalid ServiceCredentialMaxTTL %s", bad)
			} else if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("New(%s) class = %v (%v); want invalid_input", bad, class, err)
			}
		}
	})

	t.Run("tokenBounds always keeps MaxTTL at 8h", func(t *testing.T) {
		cfg := Config{ServiceCredentialMaxTTL: 90 * 24 * time.Hour}
		bounds := cfg.tokenBounds()
		if bounds.MaxTTL != 8*time.Hour {
			t.Fatalf("MaxTTL = %s; want 8h", bounds.MaxTTL)
		}
		if bounds.ServiceMaxTTL != 90*24*time.Hour {
			t.Fatalf("ServiceMaxTTL = %s; want 90d", bounds.ServiceMaxTTL)
		}
	})
}
