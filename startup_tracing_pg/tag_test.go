package startup_tracing_pg

import (
	"strings"
	"testing"
)

func noSkip(string) bool { return false }

func TestFindOutsideCallerShortensToPackageAndFunction(t *testing.T) {
	tag := findOutsideCaller(noSkip)

	if strings.Contains(tag, "/") {
		t.Fatalf("tag still carries the module path: %q", tag)
	}
	// this test file lives inside startup itself, so the first non-startup frame
	// is the test runner - short form, module path stripped.
	if tag != "testing.tRunner" {
		t.Fatalf("unexpected tag: %q", tag)
	}
}

func TestFindOutsideCallerNeverReturnsGoroutineRoot(t *testing.T) {
	// every frame skipped emulates a stack that is framework code all the way
	// down to runtime.goexit.
	tag := findOutsideCaller(func(string) bool { return true })

	if tag != "" {
		t.Fatalf("expected no caller, got %q", tag)
	}
}

func TestFindOutsideCallerSkipsRuntimeFrames(t *testing.T) {
	tag := findOutsideCaller(func(name string) bool {
		return !strings.HasPrefix(name, "runtime.")
	})

	if tag != "" {
		t.Fatalf("runtime frame reported as caller: %q", tag)
	}
}
