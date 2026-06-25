package startup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"reflect"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/go-playground/validator/v10"
	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
)

var log = slog.With(slog.String("system", "startup"))

func MustParseCommandLine(ctx context.Context, opts any) {
	MustParseCommandLineWithOptions(ctx, opts, flags.HelpFlag|flags.PassDoubleDash)
}

func MustParseCommandLineWithOptions(ctx context.Context, opts any, options flags.Options) {
	if err := ParseCommandLineWithOptions(ctx, opts, options); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func ParseCommandLine(ctx context.Context, opts any) error {
	return ParseCommandLineWithOptions(ctx, opts, flags.HelpFlag|flags.PassDoubleDash)
}

// ParseCommandLineWithOptions Parses command line.
func ParseCommandLineWithOptions(ctx context.Context, opts any, options flags.Options) error {
	if reflect.ValueOf(opts).Kind() != reflect.Pointer {
		return errors.New("options parameter must be pointer")
	}

	envFile := os.Getenv("ENV_FILE")
	if envFile == "" {
		envFile = ".env"
	}

	if err := godotenv.Load(envFile); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("load env file %q: %w", envFile, err)
		}
	}

	if options&flags.IgnoreUnknown != 0 {
		log.Warn("flags.IgnoreUnknown is set, unknown options are ignored.")
	}

	parser := flags.NewParser(opts, options)
	parser.NamespaceDelimiter = "-"

	args, err := parser.Parse()
	if err != nil {
		return err
	}

	if len(args) > 0 && (options&flags.IgnoreUnknown) != flags.None {
		log.Warn("Found ignored arguments", slog.Any("args", args))
	}

	// validate all input values after argument parsing
	v := validator.New()

	// validate host:port values
	_ = v.RegisterValidation("hostport", func(fl validator.FieldLevel) bool {
		value := fl.Field().Interface().(string)
		_, _, err := net.SplitHostPort(value)
		return err == nil
	})

	if err := v.Struct(opts); err != nil {
		return fmt.Errorf("validate options struct: %w", err)
	}

	seen := make(map[reflect.Type]reflect.Value)

	// now do the initialization for all fields
	value := reflect.ValueOf(opts).Elem()
	for _, fieldValue := range value.Fields() {
		if fieldValue.Kind() != reflect.Struct {
			continue
		}

		// we remember the values we've seen so we can inject those into
		// the Initializer() functions
		seen[fieldValue.Type()] = fieldValue
		seen[reflect.PointerTo(fieldValue.Type())] = fieldValue.Addr()

		if init := findInitializerMethod(fieldValue); init.IsValid() {
			var inputValues []reflect.Value

			initType := init.Type()
			for in := range initType.Ins() {
				var inputValue reflect.Value

				switch {
				case in == reflect.TypeFor[context.Context]():
					inputValue = reflect.ValueOf(ctx)

				case seen[in].IsValid():
					inputValue = seen[in]

				case in.Kind() == reflect.Pointer:
					// get T instead of *T
					inType := in.Elem()

					// pointers indicate optional values
					if value := seen[inType]; value.IsValid() {
						// set inputValue to (*T)(&value)
						inputValue = value.Addr()
					} else {
						// set inputValue to (*T)(nil)
						inputValue = reflect.New(in).Elem()
					}

				default:
					startup_base.Panicf("Can not find value of type %q to inject into %q",
						in.String(), fieldValue.Type())
				}

				inputValues = append(inputValues, inputValue)
			}

			if _, ok := fieldValue.Interface().(startup_base.BaseOptions); !ok {
				log.Info("Calling Initialize()", slog.String("type", fieldValue.Type().String()))
			}

			init.Call(inputValues)
		}
	}

	return nil
}

func findInitializerMethod(v reflect.Value) reflect.Value {
	m := v.MethodByName("Initialize")
	if !m.IsValid() && v.CanAddr() {
		m = v.Addr().MethodByName("Initialize")
	}

	return m
}

type URL struct {
	*url.URL
}

func (flag *URL) MarshalFlag() (string, error) {
	if flag.URL == nil {
		return "", errors.New("url flag not set")
	} else {
		return flag.String(), nil
	}
}

func (flag *URL) UnmarshalFlag(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return err
	}

	if parsed.Scheme == "" {
		return errors.New("url is missing a scheme")
	}

	if parsed.Hostname() == "" {
		return errors.New("url is missing a hostname")
	}

	flag.URL = parsed
	return err
}
