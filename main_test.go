package startup

import (
	"errors"
	"os"
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
				err = ParseCommandLineWithOptions(tt.args.opts, tt.args.options)
			} else {
				err = ParseCommandLineWithOptions(&tt.args.opts, tt.args.options)
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
