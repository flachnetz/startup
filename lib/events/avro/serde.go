package avro

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

var (
	ErrPayloadToShort   = errors.New("payload too short")
	ErrInvalidMagicByte = errors.New("invalid magic byte value")
)

type Event interface {
	// Schema returns the avro schema of this event
	Schema() string

	// Serialize writes the event (in avro format) to the given writer.
	Serialize(io.Writer) error
}

type Deserializer[E any] func(r io.Reader, schema string) (E, error)

func DeserializeWithSchema[E any](schemas confluent.Client, deser Deserializer[E], payload []byte) (E, error) {
	if len(payload) < 5 {
		var zero E
		return zero, ErrPayloadToShort
	}

	// magic byte, must be zero
	if payload[0] != 0 {
		var zero E
		return zero, ErrInvalidMagicByte
	}

	// then we have the id
	schemaId := binary.BigEndian.Uint32(payload[1:])

	schema, err := schemas.GetBySubjectAndID("", int(schemaId))
	if err != nil {
		var zero E
		return zero, fmt.Errorf("lookup schema for schemaId=%d", schemaId)
	}

	// deserialize
	r := bytes.NewReader(payload[5:])
	return deser(r, schema.Schema)
}

func SerializeWithSchema(client confluent.Client, event Event) ([]byte, error) {
	eventType := EventTypeOf(event)

	info := confluent.SchemaInfo{Schema: event.Schema()}
	schemaId, err := client.Register(eventType, info, true)
	if err != nil {
		return nil, fmt.Errorf("register schema %T: %w", eventType, err)
	}

	return SerializeWithSchemaId(uint32(schemaId), event)
}

func SerializeWithSchemaId(schemaId uint32, event Event) ([]byte, error) {
	var bufSchemaId [4]byte
	binary.BigEndian.PutUint32(bufSchemaId[:], schemaId)

	var buf bytes.Buffer
	buf.WriteByte(0)
	buf.Write(bufSchemaId[:])
	if err := event.Serialize(&buf); err != nil {
		return nil, fmt.Errorf("serialize event to avro: %w", err)
	}

	return buf.Bytes(), nil
}

func EventTypeOf(event Event) string {
	type unwrapper interface {
		Unwrap() Event
	}

	// unwrap event
	for {
		msg, ok := event.(unwrapper)
		if !ok {
			break
		}

		event = msg.Unwrap()
	}

	eventType := reflect.TypeOf(event)
	for eventType.Kind() == reflect.Pointer {
		eventType = eventType.Elem()
	}

	return eventType.Name()
}
