package startup

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/go-playground/validator.v9"
	"net"
	"net/url"
	"os"
	"reflect"
)

var log = logrus.WithField("prefix", "startup")

func init() {
	if os.Getenv("STARTUP_VERBOSE") == "true" {
		logrus.SetLevel(logrus.DebugLevel)
	}
}

func MustParseCommandLine(opts interface{}) {
	if err := ParseCommandLine(opts); err != nil {
		cause := errors.Cause(err)

		if cause, ok := cause.(*flags.Error); ok && cause.Type == flags.ErrHelp {
			fmt.Fprintln(os.Stdout, cause)
		} else {
			fmt.Fprintln(os.Stderr, err)
		}

		os.Exit(1)
	}
}

// Parses command line.
func ParseCommandLine(opts interface{}) error {
	if reflect.ValueOf(opts).Kind() != reflect.Ptr {
		return errors.New("options parameter must be pointer")
	}

	parser := flags.NewParser(opts, flags.HelpFlag|flags.PassDoubleDash)
	parser.NamespaceDelimiter = "-"

	if _, err := parser.Parse(); err != nil {
		return err
	}

	// validate all input values after argument parsing
	v := validator.New()

	// validate host:port values
	v.RegisterValidation("hostport", func(fl validator.FieldLevel) bool {
		value := fl.Field().Interface().(string)
		_, _, err := net.SplitHostPort(value)
		return err == nil
	})

	if err := v.Struct(opts); err != nil {
		return errors.WithMessage(err, "validate options struct")
	}

	seen := make(map[reflect.Type]reflect.Value)

	// now do the initialization for all fields
	value := reflect.ValueOf(opts).Elem()
	for idx := 0; idx < value.NumField(); idx++ {
		fieldValue := value.Field(idx)
		if fieldValue.Kind() != reflect.Struct {
			continue
		}

		// we remember the values we've seen so we can inject those into
		// the Initializer() functions
		seen[fieldValue.Type()] = fieldValue
		seen[reflect.PtrTo(fieldValue.Type())] = fieldValue.Addr()

		if init := findInitializerMethod(fieldValue); init.IsValid() {
			var inputValues []reflect.Value

			initType := init.Type()
			for idx := 0; idx < initType.NumIn(); idx++ {
				inputValue := seen[initType.In(idx)]
				if !inputValue.IsValid() {
					Panicf("Can not find value of type %s to inject into %s",
						initType.In(idx).String(), fieldValue.Type())
				}

				inputValues = append(inputValues, inputValue)
			}

			log.Infof("Calling %s.Initialize()", fieldValue.Type().String())
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

type FlagURL struct {
	*url.URL
}

func (flag *FlagURL) MarshalFlag() (string, error) {
	if flag.URL == nil {
		return "", errors.New("url flag not set")
	} else {
		return flag.String(), nil
	}
}

func (flag *FlagURL) UnmarshalFlag(value string) error {
	parsed, err := url.Parse(value)
	flag.URL = parsed
	return err
}
