package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"log/slog"

	"github.com/pkg/errors"
)

type EventSource struct {
	Record any
	Schema string
}

type Converter struct {
	log      *slog.Logger
	registry *SchemaRegistry
	options  ConverterOptions
}

type ConverterOptions struct {
	AvroNamespace string // prefix for namespace prefix which will be used to identify self defined records
	ToLowerCase   bool   // map all field names to lower case
}

func NewConverter(registry *SchemaRegistry, options ConverterOptions) *Converter {
	return &Converter{log: slog.With(slog.String("prefix", "avro-converter")), registry: registry, options: options}
}

func (c *Converter) Parse(data []byte) (map[string]any, *EventSource, error) {
	if bytes.HasPrefix(data, []byte("Obj\x01")) {
		// This isn't used anymore, i think.
		return nil, nil, errors.New("events in avro container format not supported")
	}

	if len(data) >= 5 && data[0] == 0 {
		// confluent format: convert 4 byte integer to schema key string
		schemaId := binary.BigEndian.Uint32(data[1:5])
		return c.decode(strconv.Itoa(int(schemaId)), data[5:])
	}

	return nil, nil, fmt.Errorf("parse event %s", string(data))
}

/**
 * Checks if the given number of bytes only contain hexadecimal characters
 */
func (c *Converter) hexadecimalCharsOnly(bytes []byte) bool {
	for _, by := range bytes {
		hexChar := (by >= 'a' && by <= 'f') || (by >= '0' && by <= '9')
		if !hexChar {
			return false
		}
	}

	return true
}

func (c *Converter) decode(hash string, data []byte) (map[string]any, *EventSource, error) {
	// get the codec for the provided hash
	codec, err := c.registry.Get(hash)
	if err != nil {
		return nil, nil, err
	}

	// use the codec to decode the data bytes
	original, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, nil, err
	}

	// convert form "avro native" to a clean go value.
	parsed := c.ConvertAvroToGo(original)

	return parsed.(map[string]any), &EventSource{original, codec.Schema()}, nil
}

func (c *Converter) ConvertAvroToGo(input any) any {
	switch input := input.(type) {
	case map[string]any:
		if result, ok := c.simplifyAvroType(input); ok {
			return c.ConvertAvroToGo(result)
		}

		result := make(map[string]any, len(input))

		for key, value := range input {
			if c.options.ToLowerCase {
				key = strings.ToLower(key)
			}
			result[key] = c.ConvertAvroToGo(value)
		}

		return result

	case []any:
		result := make([]any, 0, len(input))
		for _, value := range input {
			result = append(result, c.ConvertAvroToGo(value))
		}

		return result

	default:
		return input
	}
}

func (c *Converter) simplifyAvroType(value map[string]any) (any, bool) {
	if len(value) == 1 {
		for key, actualValue := range value {
			switch key {
			case "string", "boolean", "int", "long", "float", "double", "bytes", "array":
				return actualValue, true
			}

			if c.options.AvroNamespace != "" {
				if strings.HasPrefix(key, c.options.AvroNamespace) {
					return actualValue, true
				}
			}
		}
	}

	return nil, false
}
