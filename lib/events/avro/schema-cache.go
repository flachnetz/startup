package avro

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/linkedin/goavro/v2"
)

// SchemaCache is a wrapper around a ConfluentClient that caches the schemas as
// parsed goavro.Codec instances.
type SchemaCache struct {
	ConfluentClient confluent.Client

	cache sync.Map
}

func (r *SchemaCache) Get(ctx context.Context, schemaId uint32) (*goavro.Codec, error) {
	if codec, ok := r.cache.Load(schemaId); ok {
		return codec.(*goavro.Codec), nil
	}

	slog.InfoContext(
		ctx, "Lookup schema",
		slog.String("prefix", "schema"),
		slog.Int("schemaId", int(schemaId)),
	)

	var avroSchema string

	schemaInfo, err := r.ConfluentClient.GetBySubjectAndID("", int(schemaId))
	if err != nil {
		return nil, fmt.Errorf("lookup schema in confluent %d: %w", schemaId, err)
	}

	avroSchema = schemaInfo.Schema

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("parse schema to codec: %w", err)
	}

	r.cache.Store(schemaId, codec)

	return codec, nil
}
