package outburst

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
)

func TestShardFor(t *testing.T) {
	n := 4
	// a given key must always resolve to the same shard
	k := sql.NullString{String: "offer-42", Valid: true}
	got := shardFor(k, n)
	for i := 0; i < 100; i++ {
		if s := shardFor(k, n); s != got {
			t.Fatalf("non-deterministic shard: %d != %d", s, got)
		}
	}
	if got < 0 || got >= n {
		t.Fatalf("shard out of range: %d", got)
	}
	// a missing key must always land on shard 0
	if s := shardFor(sql.NullString{}, n); s != 0 {
		t.Fatalf("null key shard = %d, want 0", s)
	}
}

func TestParseNotificationJSON(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// a full payload yields both id and key with no database round-trip
	id, key, ok := parseNotification(context.Background(), log, `{"id":42,"key":"offer-7"}`)
	if !ok || id != 42 || !key.Valid || key.String != "offer-7" {
		t.Fatalf("got id=%d key=%+v ok=%v", id, key, ok)
	}

	// a JSON null key must decode to an invalid sql.NullString
	_, key, ok = parseNotification(context.Background(), log, `{"id":1,"key":null}`)
	if !ok || key.Valid {
		t.Fatalf("null key: key=%+v ok=%v", key, ok)
	}

	// broken JSON must be rejected
	if _, _, ok := parseNotification(context.Background(), log, `{bad`); ok {
		t.Fatal("malformed JSON should be skipped")
	}

	// a bare, non-JSON payload must be rejected
	if _, _, ok := parseNotification(context.Background(), log, "42"); ok {
		t.Fatal("non-JSON payload should be skipped")
	}
}
