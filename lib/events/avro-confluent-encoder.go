package events

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/Landoop/schema-registry"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"reflect"
	"sync"
)

type avroConfluentEncoder struct {
	log      logrus.FieldLogger
	registry *schemaregistry.Client

	cacheMutex sync.Mutex
	cache      map[string]uint32
}

func NewAvroConfluentEncoder(registry *schemaregistry.Client) Encoder {
	return &avroConfluentEncoder{
		log:      logrus.WithField("prefix", "confluent"),
		registry: registry,
		cache:    map[string]uint32{},
	}
}

func (enc *avroConfluentEncoder) Encode(event Event) ([]byte, error) {
	gid, err := enc.registerSchema(event)
	if err != nil {
		return nil, errors.WithMessage(err, "register avro schema")
	}

	// encode id in 4 bytes
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], gid)

	// write the magic byte followed by the 4 byte id.
	// see https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html
	// for a description of the format.
	var buffer bytes.Buffer
	buffer.WriteByte(0)
	buffer.Write(buf[:])

	// serialize the event
	if err := event.Serialize(&buffer); err != nil {
		return nil, errors.WithMessage(err, "encoding avro event")
	}

	return buffer.Bytes(), nil
}

func (enc *avroConfluentEncoder) registerSchema(event Event) (uint32, error) {
	// lookup in cache first
	enc.cacheMutex.Lock()
	cached, ok := enc.cache[event.Schema()]
	enc.cacheMutex.Unlock()

	if ok {
		return cached, nil
	}

	subject := nameOf(event)
	enc.log.Debugf("Registering schema for subject %s", subject)

	schemaId, err := enc.registry.RegisterNewSchema(subject, event.Schema())
	if err != nil {
		return 0, errors.WithMessage(err, "register new schema")
	}

	// cache it for lookups
	enc.cacheMutex.Lock()
	enc.cache[event.Schema()] = uint32(schemaId)
	enc.cacheMutex.Unlock()

	return uint32(schemaId), nil
}

func (enc *avroConfluentEncoder) Close() error {
	return nil
}

func nameOf(event Event) string {
	// try to take the Name of the schema
	var schema struct{ Name string }
	if json.Unmarshal([]byte(event.Schema()), &schema) == nil && schema.Name != "" {
		return schema.Name
	}

	// get the event class
	eventType := reflect.ValueOf(event).Type()
	for eventType.Kind() == reflect.Ptr || eventType.Kind() == reflect.Interface {
		eventType = eventType.Elem()
	}

	// and take the name of it
	name := eventType.Name()
	if name != "" {
		return name
	}

	return "GoAvroEvent"
}
