package startup

import (
	"context"
	"errors"
	"fmt"
	"iter"
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
		if flagError, ok := errors.AsType[*flags.Error](err); ok && errors.Is(flagError.Type, flags.ErrUnknownFlag) {
			p := flags.NewParser(opts, options)
			p.NamespaceDelimiter = "-"
			p.WriteHelp(os.Stderr)
		}
		os.Exit(1)
	}
}

// defaultEnvFile is the dotenv file read when ENV_FILE is unset.
const defaultEnvFile = ".env"

func ParseCommandLine(ctx context.Context, opts any) error {
	return ParseCommandLineWithOptions(ctx, opts, flags.HelpFlag|flags.PassDoubleDash)
}

// ParseCommandLineWithOptions Parses command line.
func ParseCommandLineWithOptions(ctx context.Context, opts any, options flags.Options) error {
	if reflect.ValueOf(opts).Kind() != reflect.Pointer {
		return errors.New("options parameter must be pointer")
	}

	propagateInputs(opts)

	if err := loadEnvFile(); err != nil {
		return err
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
	if err := validateOptions(opts); err != nil {
		return err
	}

	initializeFields(ctx, opts)

	return nil
}

// loadEnvFile loads the dotenv file named by ENV_FILE, defaulting to
// defaultEnvFile. A missing file is not an error - the environment alone is a
// valid setup.
func loadEnvFile() error {
	envFile := os.Getenv("ENV_FILE")
	if envFile == "" {
		envFile = defaultEnvFile
	}

	if err := godotenv.Load(envFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load env file %q: %w", envFile, err)
	}

	return nil
}

// validateOptions runs struct validation on the parsed options, including the
// custom "hostport" rule.
func validateOptions(opts any) error {
	v := validator.New()

	// validate host:port values
	_ = v.RegisterValidation("hostport", func(fl validator.FieldLevel) bool {
		value, ok := fl.Field().Interface().(string)
		if !ok {
			return false
		}

		_, _, err := net.SplitHostPort(value)

		return err == nil
	})

	if err := v.Struct(opts); err != nil {
		return fmt.Errorf("validate options struct: %w", err)
	}

	return nil
}

// initializeFields calls Initialize() on every struct field that has one, in
// declaration order, injecting the option structs seen so far.
func initializeFields(ctx context.Context, opts any) {
	seen := make(map[reflect.Type]reflect.Value)

	value := reflect.ValueOf(opts).Elem()
	for fieldValue := range fieldsIter(value) {
		if fieldValue.Kind() != reflect.Struct {
			continue
		}

		// we remember the values we've seen so we can inject those into
		// the Initializer() functions
		seen[fieldValue.Type()] = fieldValue
		seen[reflect.PointerTo(fieldValue.Type())] = fieldValue.Addr()

		init := findInitializerMethod(fieldValue)
		if !init.IsValid() {
			continue
		}

		if _, ok := fieldValue.Interface().(startup_base.BaseOptions); !ok {
			log.Info("Calling Initialize()", slog.String("type", fieldValue.Type().String()))
		}

		init.Call(initializerArgs(ctx, init, fieldValue, seen))
	}
}

// initializerArgs resolves the arguments of one Initialize() method from the
// option structs seen so far.
func initializerArgs(ctx context.Context, init reflect.Value, owner reflect.Value, seen map[reflect.Type]reflect.Value) []reflect.Value {
	initType := init.Type()

	inputValues := make([]reflect.Value, 0, initType.NumIn())

	for in := range initType.Ins() {
		inputValues = append(inputValues, initializerArg(ctx, in, owner, seen))
	}

	return inputValues
}

// initializerArg resolves a single Initialize() parameter. A pointer parameter
// marks an optional value and stays nil when nothing of that type was seen.
func initializerArg(ctx context.Context, in reflect.Type, owner reflect.Value, seen map[reflect.Type]reflect.Value) reflect.Value {
	switch {
	case in == reflect.TypeFor[context.Context]():
		return reflect.ValueOf(ctx)

	case seen[in].IsValid():
		return seen[in]

	case in.Kind() == reflect.Pointer:
		// pointers indicate optional values: get T instead of *T
		if value := seen[in.Elem()]; value.IsValid() {
			// (*T)(&value)
			return value.Addr()
		}

		// (*T)(nil)
		return reflect.New(in).Elem()

	default:
		startup_base.Panicf("Can not find value of type %q to inject into %q",
			in.String(), owner.Type())

		return reflect.Value{}
	}
}

func propagateInputs(opts any) {
	type propagateInputs interface {
		PropagateInputs()
	}

	if p, ok := opts.(propagateInputs); ok {
		p.PropagateInputs()
	}
}

func fieldsIter(value reflect.Value) iter.Seq[reflect.Value] {
	return func(yield func(reflect.Value) bool) {
		yieldFields(value, yield)
	}
}

// yieldFields yields every exported field of value, recursing into embedded
// fields first. It reports whether iteration should continue.
func yieldFields(value reflect.Value, yield func(reflect.Value) bool) bool {
	if value.Kind() != reflect.Struct {
		return true
	}

	for sf, field := range value.Fields() {
		if sf.PkgPath != "" {
			// field is not exported
			continue
		}

		// this is an embedded field, recurse
		if sf.Anonymous && !yieldFields(field, yield) {
			return false
		}

		if !yield(field) {
			return false
		}
	}

	return true
}

func findInitializerMethod(v reflect.Value) reflect.Value {
	m := v.MethodByName("Initialize")
	if !m.IsValid() && v.CanAddr() {
		m = v.Addr().MethodByName("Initialize")
	}

	if !m.IsValid() {
		return m
	}

	// Check if this method is merely promoted from an embedded field.
	// If so, skip it here — it will be found when we visit the embedded field directly.
	// We compare function pointers: if the method on the struct is the same as on
	// the embedded field, it's promoted. If the struct defines its own, it shadows
	// the embedded one and should still be called.
	if v.Kind() == reflect.Struct {
		for sf, field := range v.Fields() {
			if !sf.Anonymous {
				continue
			}
			em := field.MethodByName("Initialize")
			if !em.IsValid() && field.CanAddr() {
				em = field.Addr().MethodByName("Initialize")
			}
			if em.IsValid() && m.Pointer() == em.Pointer() {
				return reflect.Value{}
			}
		}
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
