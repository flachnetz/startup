package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

type EventSource struct {
	Record interface{}
	Schema string
}

type Converter struct {
	log      *logrus.Entry
	registry *SchemaRegistry
	options  ConverterOptions
}

type ConverterOptions struct {
	AvroNamespace string // prefix for namespace prefix which will be used to identify self defined records
	ToLowerCase   bool   // map all field names to lower case
}

func NewConverter(registry *SchemaRegistry, options ConverterOptions) *Converter {
	return &Converter{log: logrus.WithField("prefix", "avro-converter"), registry: registry, options: options}
}

func (c *Converter) Parse(data []byte) (map[string]interface{}, *EventSource, error) {
	if bytes.HasPrefix(data, []byte("Obj\x01")) {
		// This isn't used anymore, i think.
		return nil, nil, errors.New("events in avro container format not supported")
	}

	if len(data) > 32 && c.hexadecimalCharsOnly(data[0:32]) {
		// consul hash format: looks like we need to got the hash of the schema.
		return c.decode(string(data[:32]), data[32:])
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

func (c *Converter) decode(hash string, data []byte) (map[string]interface{}, *EventSource, error) {
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

	return parsed.(map[string]interface{}), &EventSource{original, codec.Schema()}, nil
}

func (c *Converter) ConvertAvroToGo(input interface{}) interface{} {
	switch input := input.(type) {
	case map[string]interface{}:
		if result, ok := c.simplifyAvroType(input); ok {
			return c.ConvertAvroToGo(result)
		}

		result := make(map[string]interface{}, len(input))

		for key, value := range input {
			if c.options.ToLowerCase {
				key = strings.ToLower(key)
			}
			result[key] = c.ConvertAvroToGo(value)
		}

		return result

	case []interface{}:
		result := make([]interface{}, 0, len(input))
		for _, value := range input {
			result = append(result, c.ConvertAvroToGo(value))
		}

		return result

	default:
		return input
	}
}

func (c *Converter) simplifyAvroType(value map[string]interface{}) (interface{}, bool) {
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
