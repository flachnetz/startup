package startup

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"

	"log/slog"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/go-playground/validator/v10"
	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
)

var log = slog.With(slog.String("system", "startup"))

func MustParseCommandLine(opts any) {
	MustParseCommandLineWithOptions(opts, flags.HelpFlag|flags.PassDoubleDash)
}

func MustParseCommandLineWithOptions(opts any, options flags.Options) {
	if err := ParseCommandLineWithOptions(opts, options); err != nil {
		if cause, ok := errors.AsType[*flags.Error](err); ok && cause.Type == flags.ErrHelp {
			_, _ = fmt.Fprintln(os.Stdout, cause)
		}

		os.Exit(1)
	}
}

func ParseCommandLine(opts any) error {
	return ParseCommandLineWithOptions(opts, flags.HelpFlag|flags.PassDoubleDash)
}

// ParseCommandLineWithOptions Parses command line.
func ParseCommandLineWithOptions(opts any, options flags.Options) error {
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
				inputValue := seen[in]
				if !inputValue.IsValid() {
					startup_base.Panicf("Can not find value of type %s to inject into %s",
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
