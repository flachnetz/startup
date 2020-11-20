package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type EventSource struct {
	Record interface{}
	Schema string
}

type Converter struct {
	log           *logrus.Entry
	registry      *SchemaRegistry
	avroNamespace string
}

func NewConverter(registry *SchemaRegistry, avroNamespace string) *Converter {
	return &Converter{log: logrus.WithField("prefix", "avro-converter"), registry: registry, avroNamespace: avroNamespace}
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
	parsed := c.convertAvroToGo(original)

	return parsed.(map[string]interface{}), &EventSource{original, codec.Schema()}, nil
}

func (c *Converter) convertAvroToGo(input interface{}) interface{} {
	switch input := input.(type) {
	case map[string]interface{}:
		if result, ok := c.simplifyAvroType(input); ok {
			return c.convertAvroToGo(result)
		}

		result := make(map[string]interface{}, len(input))

		for key, value := range input {
			result[key] = c.convertAvroToGo(value)
		}

		return result

	case []interface{}:
		result := make([]interface{}, 0, len(input))
		for _, value := range input {
			result = append(result, c.convertAvroToGo(value))
		}

		return result

	case string:
		// limit to maximum field length in elasticsearch
		if len(input) > 32760 {
			return input[:32760]
		}

		return input

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

			if strings.HasPrefix(key, c.avroNamespace) {
				return actualValue, true
			}
		}
	}

	return nil, false
}

func (c *Converter) isNamespacedAvroType(field string) bool {
	return strings.HasPrefix(field, c.avroNamespace)
}