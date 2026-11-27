package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServerServiceCredentialMaxTTLPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	if err := os.WriteFile(path, []byte(`{"service_credential_max_ttl":"7d"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("config file", func(t *testing.T) {
		options, code := parseServerOptions([]string{"--config", path}, io.Discard)
		if code != 0 {
			t.Fatalf("parse exit = %d; want 0", code)
		}
		if options.ServiceCredentialMaxTTL != 7*24*time.Hour {
			t.Fatalf("service credential max ttl = %s; want 7d", options.ServiceCredentialMaxTTL)
		}
	})

	t.Run("environment overrides the file", func(t *testing.T) {
		t.Setenv("KODEMA_SERVICE_CREDENTIAL_MAX_TTL", "14d")
		options, code := parseServerOptions([]string{"--config", path}, io.Discard)
		if code != 0 {
			t.Fatalf("parse exit = %d; want 0", code)
		}
		if options.ServiceCredentialMaxTTL != 14*24*time.Hour {
			t.Fatalf("service credential max ttl = %s; want 14d", options.ServiceCredentialMaxTTL)
		}
	})

	t.Run("flag overrides the environment", func(t *testing.T) {
		t.Setenv("KODEMA_SERVICE_CREDENTIAL_MAX_TTL", "14d")
		options, code := parseServerOptions([]string{"--config", path, "--service-credential-max-ttl", "90d"}, io.Discard)
		if code != 0 {
			t.Fatalf("parse exit = %d; want 0", code)
		}
		if options.ServiceCredentialMaxTTL != 90*24*time.Hour {
			t.Fatalf("service credential max ttl = %s; want 90d", options.ServiceCredentialMaxTTL)
		}
	})

	t.Run("Go duration flag is accepted", func(t *testing.T) {
		options, code := parseServerOptions([]string{"--service-credential-max-ttl", "48h"}, io.Discard)
		if code != 0 {
			t.Fatalf("parse exit = %d; want 0", code)
		}
		if options.ServiceCredentialMaxTTL != 48*time.Hour {
			t.Fatalf("service credential max ttl = %s; want 48h", options.ServiceCredentialMaxTTL)
		}
	})

	t.Run("unparseable or non-positive env value is refused without echoing value", func(t *testing.T) {
		for _, bad := range []string{"soon-secret-token", "0s", "-1h", "0d", "-7d"} {
			t.Setenv("KODEMA_SERVICE_CREDENTIAL_MAX_TTL", bad)
			var stderr bytes.Buffer
			if _, code := parseServerOptions(nil, &stderr); code != 2 {
				t.Fatalf("env=%q parse exit = %d; want 2", bad, code)
			}
			if !strings.Contains(stderr.String(), "KODEMA_SERVICE_CREDENTIAL_MAX_TTL") {
				t.Fatalf("env=%q refusal = %q; want setting name", bad, stderr.String())
			}
			if bad == "soon-secret-token" && strings.Contains(stderr.String(), bad) {
				t.Fatalf("env=%q refusal echoed raw value: %q", bad, stderr.String())
			}
		}
	})
}

func TestServerServiceCredentialMaxTTLWiredAcrossProfiles(t *testing.T) {
	t.Run("development profile", func(t *testing.T) {
		opts := defaultServerOptions()
		opts.Profile = "development"
		opts.DataDir = t.TempDir()
		opts.DevTokens = []string{"dev-tok"}
		opts.ServiceCredentialMaxTTL = 60 * 24 * time.Hour

		cfg, err := opts.config()
		if err != nil {
			t.Fatalf("development config: %v", err)
		}
		if cfg.ServiceCredentialMaxTTL != 60*24*time.Hour {
			t.Fatalf("development ServiceCredentialMaxTTL = %s; want 60d", cfg.ServiceCredentialMaxTTL)
		}

		opts.ServiceCredentialMaxTTL = 5 * time.Second
		if _, err := opts.config(); err == nil {
			t.Fatal("development config accepted ServiceCredentialMaxTTL below MinTTL")
		}
	})

	t.Run("production profile", func(t *testing.T) {
		getenv, kube, options := productionFixture(t)

		build, err := buildProduction(options, getenv, kube)
		if err != nil {
			t.Fatalf("buildProduction default: %v", err)
		}
		if build.Config.ServiceCredentialMaxTTL != 0 {
			t.Fatalf("default production ServiceCredentialMaxTTL = %s; want 0 (default 30d)", build.Config.ServiceCredentialMaxTTL)
		}

		// Via injected environment.
		envWithTTL := func(key string) string {
			if key == "KODEMA_SERVICE_CREDENTIAL_MAX_TTL" {
				return "45d"
			}
			return getenv(key)
		}
		build, err = buildProduction(options, envWithTTL, kube)
		if err != nil {
			t.Fatalf("buildProduction env: %v", err)
		}
		if build.Config.ServiceCredentialMaxTTL != 45*24*time.Hour {
			t.Fatalf("env production ServiceCredentialMaxTTL = %s; want 45d", build.Config.ServiceCredentialMaxTTL)
		}

		// Flag overrides env.
		options.ServiceCredentialMaxTTL = 14 * 24 * time.Hour
		build, err = buildProduction(options, envWithTTL, kube)
		if err != nil {
			t.Fatalf("buildProduction flag: %v", err)
		}
		if build.Config.ServiceCredentialMaxTTL != 14*24*time.Hour {
			t.Fatalf("flag production ServiceCredentialMaxTTL = %s; want 14d", build.Config.ServiceCredentialMaxTTL)
		}

		// Below MinTTL refused.
		options.ServiceCredentialMaxTTL = 10 * time.Second
		if _, err := buildProduction(options, getenv, kube); err == nil {
			t.Fatal("buildProduction accepted ServiceCredentialMaxTTL below MinTTL")
		}
	})

	t.Run("test profile", func(t *testing.T) {
		fixture := newTestProfileFixture(t)

		build, err := buildTestProfile(fixture.options, fixture.getenv)
		if err != nil {
			t.Fatalf("buildTestProfile default: %v", err)
		}
		if build.Config.ServiceCredentialMaxTTL != 0 {
			t.Fatalf("default test profile ServiceCredentialMaxTTL = %s; want 0 (default 30d)", build.Config.ServiceCredentialMaxTTL)
		}

		fixture.options.ServiceCredentialMaxTTL = 7 * 24 * time.Hour
		build, err = buildTestProfile(fixture.options, fixture.getenv)
		if err != nil {
			t.Fatalf("buildTestProfile configured: %v", err)
		}
		if build.Config.ServiceCredentialMaxTTL != 7*24*time.Hour {
			t.Fatalf("configured test profile ServiceCredentialMaxTTL = %s; want 7d", build.Config.ServiceCredentialMaxTTL)
		}

		fixture.options.ServiceCredentialMaxTTL = 10 * time.Second
		if _, err := buildTestProfile(fixture.options, fixture.getenv); err == nil {
			t.Fatal("buildTestProfile accepted ServiceCredentialMaxTTL below MinTTL")
		}
	})
}
