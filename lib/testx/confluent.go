package testx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/stretchr/testify/require"
)

// ConfluentRegistry is an in-memory mock of the Confluent schema registry REST API.
// It only supports schema registration and lookup by id and is meant for tests only.
type ConfluentRegistry struct {
	URL string

	testing *testing.T

	lock    sync.Mutex
	nextID  int
	schemas map[int]confluent.SchemaInfo
}

// MockConfluentRegistry starts an in-memory schema registry served over HTTP. The
// server is shut down automatically on test cleanup. Use Client to obtain a
// confluent client pointing at it.
func MockConfluentRegistry(t *testing.T) *ConfluentRegistry {
	t.Helper()

	r := &ConfluentRegistry{
		testing: t,
		nextID:  1,
		schemas: make(map[int]confluent.SchemaInfo),
	}

	mux := http.NewServeMux()

	// Register a schema under a subject: POST /subjects/{subject}/versions
	mux.HandleFunc("POST /subjects/{subject}/versions", func(w http.ResponseWriter, req *http.Request) {
		var schema confluent.SchemaInfo
		require.NoError(t, json.NewDecoder(req.Body).Decode(&schema))

		id := r.register(schema)

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]int{"id": id}))
	})

	// Lookup a schema by id: GET /schemas/ids/{id}
	mux.HandleFunc("GET /schemas/ids/{id}", func(w http.ResponseWriter, req *http.Request) {
		id, err := strconv.Atoi(req.PathValue("id"))
		require.NoError(t, err)

		schema, ok := r.get(id)
		require.Truef(t, ok, "no schema registered with id %d", id)

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(schema))
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	r.URL = server.URL
	return r
}

// register stores schema under a fresh id and returns that id. If the exact same
// schema was already registered, the existing id is reused.
func (r *ConfluentRegistry) register(schema confluent.SchemaInfo) int {
	r.lock.Lock()
	defer r.lock.Unlock()

	for id, existing := range r.schemas {
		if existing.Schema == schema.Schema {
			return id
		}
	}

	id := r.nextID
	r.nextID++
	r.schemas[id] = schema
	return id
}

// get returns the schema registered under id, if any.
func (r *ConfluentRegistry) get(id int) (confluent.SchemaInfo, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	schema, ok := r.schemas[id]
	return schema, ok
}

// Client returns a confluent schema registry client connected to this mock.
func (r *ConfluentRegistry) Client() confluent.Client {
	client, err := confluent.NewClient(confluent.NewConfig(r.URL))
	require.NoError(r.testing, err)
	return client
}
