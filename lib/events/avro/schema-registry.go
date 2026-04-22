package avro

import (
	"log/slog"
	"regexp"
	"strconv"
	"sync"

	"fmt"

	schemaregistry "github.com/Landoop/schema-registry"
	"github.com/linkedin/goavro/v2"
)

type SchemaRegistry struct {
	ConfluentClient *schemaregistry.Client

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

		avroSchema, err = r.ConfluentClient.GetSchemaByID(schemaId)
		if err != nil {
			return nil, fmt.Errorf("lookup schema in confluent %d: %w", schemaId, err)
		}

	} else {
		// still no schema? thats too bad
		return nil, fmt.Errorf("no schema found for key '%s'", key)
	}

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse schema to codec: %w", err)
	}

	r.cache.Store(key, codec)

	return codec, nil
}
