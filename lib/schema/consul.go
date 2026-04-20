package schema

import (
	"log/slog"

	consul "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

type consulSchemaRegistry struct {
	consul *consul.Client
	log    *slog.Logger
}

func NewConsulSchemaRegistry(client *consul.Client) Registry {
	return &consulSchemaRegistry{
		consul: client,
		log:    slog.With(slog.String("prefix", "schema-registry")),
	}
}

func (r *consulSchemaRegistry) Get(key string) (string, error) {
	pair, _, err := r.consul.KV().Get("avro-schemas/"+key, nil)
	if err != nil {
		return "", errors.WithMessage(err, "Could not lookup schema in consul")
	}

	if pair == nil {
		return "", errors.New("Schema for key not found: " + key)
	}

	return string(pair.Value), nil
}

func (r *consulSchemaRegistry) Set(schemaString string) (string, error) {
	hash := Hash(schemaString)

	// check if already know this hash
	key := "avro-schemas/" + hash
	kv, _, err := r.consul.KV().Get(key, &consul.QueryOptions{})
	if err != nil {
		return "", errors.WithMessage(err, "looking up avro schema in consul")
	}

	if kv == nil {
		r.log.Debug("Writing schema to consul", slog.String("hash", hash))

		// the kv entry does not exist, create it now.
		kv = &consul.KVPair{Key: key, Value: []byte(schemaString)}
		if _, err := r.consul.KV().Put(kv, &consul.WriteOptions{}); err != nil {
			return "", errors.WithMessage(err, "writing avro schema to consul")
		}
	}
	// cut the prefix
	return key[13:], nil
}

func (r *consulSchemaRegistry) Close() error {
	return nil
}
