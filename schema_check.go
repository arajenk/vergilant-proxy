package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The columns this program actually reads or writes. Checked once at startup
// so a database that predates a schema change fails immediately, with the
// missing column named, instead of turning every proxied request into a 500.
//
// If you add a column to schema.sql and start using it, add it here too. The
// list is short on purpose: it's what the two queries in db.go touch, not
// every column in the file.
var requiredColumns = map[string][]string{
	"projects": {"key", "monthly_request_limit"},
	"requests": {
		"project_key", "timestamp", "provider", "model", "status",
		"latency_ms", "first_token_ms", "input_tokens", "output_tokens",
		"estimated_cost_usd", "error",
	},
}

// checkSchema reports every required column the database is missing, in one
// error, so re-running after a fix doesn't reveal them one at a time.
func checkSchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = ANY($1)`,
		[]string{"projects", "requests"})
	if err != nil {
		return fmt.Errorf("could not read the database's own column list: %w", err)
	}
	defer rows.Close()

	found := make(map[string]bool)
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return err
		}
		found[table+"."+column] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var missing []string
	for table, columns := range requiredColumns {
		for _, column := range columns {
			if !found[table+"."+column] {
				missing = append(missing, table+"."+column)
			}
		}
	}
	if len(missing) > 0 {
		// Sorted so the message is stable across runs; Go randomizes map order.
		slices.Sort(missing)
		return fmt.Errorf("database is missing %s; apply schema.sql",
			strings.Join(missing, ", "))
	}
	return nil
}
