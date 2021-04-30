package avro

import (
	schemaregistry "github.com/Landoop/schema-registry"
	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"regexp"
	"strconv"
	"sync"

	"github.com/flachnetz/startup/v2/lib/schema"
)

type SchemaRegistry struct {
	Consul          schema.Registry
	ConfluentClient *schemaregistry.Client

	cache sync.Map
}

func (r *SchemaRegistry) Get(key string) (*goavro.Codec, error) {
	if codec, ok := r.cache.Load(key); ok {
		return codec.(*goavro.Codec), nil
	}

	logrus.WithField("prefix", "schema").Infof("Lookup schema for key='%s'", key)

	var avroSchema string

	// try consul if key length is okay
	if r.Consul != nil && len(key) == 32 {
		var err error

		avroSchema, err = r.Consul.Get(key)
		if err != nil {
			return nil, errors.WithMessage(err, "lookup in consul")
		}

	} else if r.ConfluentClient != nil && regexp.MustCompile(`^[0-9]+`).MatchString(key) {
		// try confluent registry next
		schemaId, err := strconv.Atoi(key)
		if err != nil {
			return nil, errors.WithMessage(err, "parse schema id as integer")
		}

		avroSchema, err = r.ConfluentClient.GetSchemaByID(schemaId)
		if err != nil {
			return nil, errors.WithMessagef(err, "lookup schema in confluent %d", schemaId)
		}

	} else {
		// still no schema? thats too bad
		return nil, errors.Errorf("no schema found for key '%s'", key)
	}

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, errors.WithMessage(err, "parse schema to codec")
	}

	r.cache.Store(key, codec)

	return codec, nil
}
