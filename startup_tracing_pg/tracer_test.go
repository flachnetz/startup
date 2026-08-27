package startup_tracing_pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryResource(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "insert",
			query: `INSERT INTO "order_service_history" ("timestamp", payload) VALUES ($1, $2)`,
			want:  "INSERT order_service_history",
		},
		{
			name:  "select with alias and joins",
			query: `SELECT o.id FROM "orders" o JOIN order_items i ON i.order_id = o.id WHERE o.id = $1`,
			want:  "SELECT orders",
		},
		{
			name:  "update",
			query: `UPDATE orders SET status = $1 WHERE id = $2`,
			want:  "UPDATE orders",
		},
		{
			name:  "delete",
			query: `DELETE FROM order_items WHERE order_id = $1`,
			want:  "DELETE order_items",
		},
		{
			name:  "schema qualified table keeps the qualifier",
			query: `SELECT 1 FROM public.orders`,
			want:  "SELECT public.orders",
		},
		{
			name:  "cte",
			query: `WITH picked AS (SELECT id FROM orders LIMIT 1) UPDATE orders SET status = 'x'`,
			want:  "UPDATE orders",
		},
		{
			name:  "statement without a table falls back to the keyword",
			query: `BEGIN`,
			want:  "BEGIN",
		},
		{
			name:  "unrecognisable statement stays bounded",
			query: `$$`,
			want:  "query",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, queryResource(c.query))
		})
	}
}
