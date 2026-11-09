package startup_base

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestBootstrapHandlerLogsJSONWhenLogJSONIsSet(t *testing.T) {
	t.Setenv("LOG_JSON", "true")

	var buf bytes.Buffer
	slog.New(bootstrapHandler(&buf)).Info("starting")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("expected a JSON line, got %q: %v", buf.String(), err)
	}

	if line["level"] != "INFO" || line["msg"] != "starting" {
		t.Fatalf("unexpected line: %v", line)
	}
}

func TestBootstrapHandlerLogsTextWithoutLogJSON(t *testing.T) {
	for _, value := range []string{"", "false", "not-a-bool"} {
		t.Setenv("LOG_JSON", value)

		var buf bytes.Buffer
		slog.New(bootstrapHandler(&buf)).Info("starting")

		if !strings.Contains(buf.String(), "level=INFO ") || !strings.HasSuffix(buf.String(), "msg=starting\n") {
			t.Fatalf("LOG_JSON=%q: expected a text line, got %q", value, buf.String())
		}
	}
}

func TestIsProductionRecognisesProductionAliases(t *testing.T) {
	previous := GetEnvironment()
	t.Cleanup(func() { SetEnvironment(previous) })

	for _, env := range []string{"production", "prod", "prd", "live", "PRD", "Prd"} {
		SetEnvironment(env)
		if !IsProduction() {
			t.Errorf("IsProduction() = false for environment %q", env)
		}
	}

	for _, env := range []string{"", "dev", "staging", "stage", "test", "prdx", "preprod"} {
		SetEnvironment(env)
		if IsProduction() {
			t.Errorf("IsProduction() = true for environment %q", env)
		}
	}
}
