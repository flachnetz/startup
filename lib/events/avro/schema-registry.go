package avro

import (
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"sync"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
)

type SchemaRegistry struct {
	ConfluentClient confluent.Client

	cache sync.Map
}

func (r *SchemaRegistry) Get(key string) (*goavro.Codec, error) {
	if codec, ok := r.cache.Load(key); ok {
		return codec.(*goavro.Codec), nil
	}

	slog.Info("Lookup schema", slog.String("prefix", "schema"), slog.String("key", key))

	var avroSchema string

	if regexp.MustCompile(`^[0-9]+`).MatchString(key) {
		// try confluent registry next
		schemaId, err := strconv.Atoi(key)
		if err != nil {
			return nil, fmt.Errorf("parse schema id as integer: %w", err)
		}

		schemaInfo, err := r.ConfluentClient.GetBySubjectAndID("", schemaId)
		if err != nil {
			return nil, fmt.Errorf("lookup schema in confluent %d: %w", schemaId, err)
		}

		avroSchema = schemaInfo.Schema

	} else {
		// still no schema? that is too bad
		return nil, fmt.Errorf("no schema found for key %q", key)
	}

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse schema to codec: %w", err)
	}

	r.cache.Store(key, codec)

	return codec, nil
}
