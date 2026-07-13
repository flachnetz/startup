package startup

import (
	"errors"
	"os"
	"reflect"
	"testing"

	goflags "github.com/jessevdk/go-flags"
)

type testStruct struct {
	PostgresURL string `long:"postgres-url" env:"POSTGRES_URL" description:"Read data from postgres."`
}

func TestParseCommandLineWithOptions(t *testing.T) {
	type args struct {
		opts       testStruct
		options    goflags.Options
		osArgs     []string
		nonPointer bool
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		checkError func(t *testing.T, err error)
	}{
		{
			name: "ignored args",
			args: args{
				opts:    testStruct{PostgresURL: "url"},
				options: goflags.IgnoreUnknown,
				osArgs:  []string{"cmd", "-user=bla", "--postgres-url=fancyurl"},
			},
			wantErr: false,
		},
		{
			name: "valid args",
			args: args{
				opts:    testStruct{},
				options: goflags.HelpFlag | goflags.PassDoubleDash,
				osArgs:  []string{"cmd", "--postgres-url=myurl"},
			},
			wantErr: false,
		},
		{
			name: "help flag returns flags.Error with ErrHelp",
			args: args{
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
			args: args{
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
			args: args{
				opts:       testStruct{},
				options:    goflags.HelpFlag,
				osArgs:     []string{"cmd"},
				nonPointer: true,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
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
