package startup

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	goflags "github.com/jessevdk/go-flags"
	"github.com/stretchr/testify/require"
)

type testStruct struct {
	PostgresURL string `long:"postgres-url" env:"POSTGRES_URL" description:"Read data from postgres."`
}

// parseArgs is one command line invocation for the table below.
type parseArgs struct {
	opts       testStruct
	options    goflags.Options
	osArgs     []string
	nonPointer bool
}

// parseCases lives outside the test so the table stays readable and the test
// body keeps to the loop.
var parseCases = []struct {
	name       string
	args       parseArgs
	wantErr    bool
	checkError func(t *testing.T, err error)
}{
	{
		name: "ignored args",
		args: parseArgs{
			opts:    testStruct{PostgresURL: "url"},
			options: goflags.IgnoreUnknown,
			osArgs:  []string{"cmd", "-user=bla", "--postgres-url=fancyurl"},
		},
		wantErr: false,
	},
	{
		name: "valid args",
		args: parseArgs{
			opts:    testStruct{},
			options: goflags.HelpFlag | goflags.PassDoubleDash,
			osArgs:  []string{"cmd", "--postgres-url=myurl"},
		},
		wantErr: false,
	},
	{
		name: "help flag returns flags.Error with ErrHelp",
		args: parseArgs{
			opts:    testStruct{},
			options: goflags.HelpFlag,
			osArgs:  []string{"cmd", "--help"},
		},
		wantErr: true,
		checkError: func(t *testing.T, err error) {
			t.Helper()
			var flagsErr *goflags.Error
			if !errors.As(err, &flagsErr) {
				t.Fatalf("expected *flags.Error, got %T", err)
			}
			if flagsErr.Type != goflags.ErrHelp {
				t.Errorf("expected ErrHelp, got %v", flagsErr.Type)
			}
		},
	},
	{
		name: "unknown flag without IgnoreUnknown returns error",
		args: parseArgs{
			opts:    testStruct{},
			options: goflags.HelpFlag | goflags.PassDoubleDash,
			osArgs:  []string{"cmd", "--unknown-flag=value"},
		},
		wantErr: true,
		checkError: func(t *testing.T, err error) {
			t.Helper()
			var flagsErr *goflags.Error
			if !errors.As(err, &flagsErr) {
				t.Fatalf("expected *flags.Error, got %T", err)
			}
			if flagsErr.Type == goflags.ErrHelp {
				t.Error("expected non-help error type")
			}
		},
	},
	{
		name: "non-pointer opts returns error",
		args: parseArgs{
			opts:       testStruct{},
			options:    goflags.HelpFlag,
			osArgs:     []string{"cmd"},
			nonPointer: true,
		},
		wantErr: true,
	},
}

func TestParseCommandLineWithOptions(t *testing.T) {
	for _, tt := range parseCases {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = tt.args.osArgs

			var err error
			if tt.args.nonPointer {
				err = ParseCommandLineWithOptions(t.Context(), tt.args.opts, tt.args.options)
			} else {
				err = ParseCommandLineWithOptions(t.Context(), &tt.args.opts, tt.args.options)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCommandLineWithOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.checkError != nil && err != nil {
				tt.checkError(t, err)
			}
		})
	}
}

// initTracker records calls to Initialize() in order.
var initTracker []string

type ChildWithInit struct {
	Value string
}

func (c *ChildWithInit) Initialize() {
	initTracker = append(initTracker, "childWithInit")
}

type ParentWithEmbeddedInit struct {
	ChildWithInit
	Other string
}

func TestFieldsIter_InitializeNotCalledTwiceForEmbedded(t *testing.T) {
	initTracker = nil

	// Wrap parentWithEmbeddedInit in an outer struct so it becomes a field
	// that fieldsIter will visit.
	type Outer struct {
		ParentWithEmbeddedInit
	}

	outer := Outer{}
	val := reflect.ValueOf(&outer).Elem()

	var initialized []string
	for field := range fieldsIter(val) {
		if field.Kind() != reflect.Struct {
			continue
		}

		if init := findInitializerMethod(field); init.IsValid() {
			initialized = append(initialized, field.Type().Name())
			init.Call(nil)
		}
	}

	// Initialize() should be found exactly once, on childWithInit,
	// not again on parentWithEmbeddedInit (where it is only promoted).
	if len(initialized) != 1 {
		t.Fatalf("expected Initialize() found once, got %d times on: %v", len(initialized), initialized)
	}
	if initialized[0] != "ChildWithInit" {
		t.Errorf("expected Initialize() on childWithInit, got %s", initialized[0])
	}
	if len(initTracker) != 1 {
		t.Errorf("expected Initialize() called once, got %d calls: %v", len(initTracker), initTracker)
	}
}

func TestFieldsIter_EmbeddedInitializedBeforeParent(t *testing.T) {
	type GrandChild struct {
		X string
	}

	type Mid struct {
		GrandChild
		Y string
	}

	type Top struct {
		Mid
		Z string
	}

	top := Top{}
	var structOrder []string
	for field := range fieldsIter(reflect.ValueOf(top)) {
		if field.Kind() == reflect.Struct {
			structOrder = append(structOrder, field.Type().Name())
		}
	}

	// fieldsIter recurses into embedded fields first, so GrandChild must
	// appear before Mid (its embedding parent).
	gcIdx := -1
	midIdx := -1
	for i, name := range structOrder {
		if name == "GrandChild" && gcIdx == -1 {
			gcIdx = i
		}
		if name == "Mid" && midIdx == -1 {
			midIdx = i
		}
	}

	if gcIdx == -1 {
		t.Fatalf("GrandChild not found in iteration, got: %v", structOrder)
	}
	if midIdx == -1 {
		t.Fatalf("Mid not found in iteration, got: %v", structOrder)
	}
	if gcIdx >= midIdx {
		t.Errorf("expected GrandChild (idx %d) before Mid (idx %d); Initialize() of embedded field must be called first. order: %v",
			gcIdx, midIdx, structOrder)
	}
}

// TestLoadEnvFile covers the three cases loadEnvFile has to handle: an explicit
// ENV_FILE that exists, one that does not exist (not an error), and a broken
// file (an error).
func TestLoadEnvFile(t *testing.T) {
	t.Run("reads the file named by ENV_FILE", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "custom.env")
		require.NoError(t, os.WriteFile(file, []byte("STARTUP_TEST_VALUE=from-file\n"), 0o600))

		t.Setenv("ENV_FILE", file)
		require.NoError(t, loadEnvFile())
		require.Equal(t, "from-file", os.Getenv("STARTUP_TEST_VALUE"))
	})

	t.Run("a missing file is not an error", func(t *testing.T) {
		t.Setenv("ENV_FILE", filepath.Join(t.TempDir(), "does-not-exist"))
		require.NoError(t, loadEnvFile())
	})

	t.Run("an unparsable file is an error", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "broken.env")
		require.NoError(t, os.WriteFile(file, []byte("not a key value line\n"), 0o600))

		t.Setenv("ENV_FILE", file)
		require.Error(t, loadEnvFile())
	})
}

// TestValidateOptions covers the custom "hostport" validation rule.
func TestValidateOptions(t *testing.T) {
	type opts struct {
		Address string `validate:"hostport"`
	}

	require.NoError(t, validateOptions(&opts{Address: "localhost:9092"}))
	require.Error(t, validateOptions(&opts{Address: "localhost"}))
}
