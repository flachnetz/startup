package startup

import (
	"os"
	"testing"

	"github.com/jessevdk/go-flags"
)

type testStruct struct {
	PostgresURL string `long:"postgres-url" description:"Read data from postgres."`
}

func TestParseCommandLineWithOptions(t *testing.T) {
	type args struct {
		opts    testStruct
		options flags.Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "ignored args",
			args: args{
				opts:    testStruct{PostgresURL: "url"},
				options: flags.IgnoreUnknown,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = []string{"cmd", "-user=bla", "--postgres-url=fancyurl"}
			if err := ParseCommandLineWithOptions(&tt.args.opts, tt.args.options); (err != nil) != tt.wantErr {
				t.Errorf("ParseCommandLineWithOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
