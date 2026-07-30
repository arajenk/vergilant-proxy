package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// What the two queries in db.go touch, not every column in schema.sql. Checked
// at startup so a database that predates a schema change fails immediately,
// naming the missing column, instead of 500ing every request.
//
// Add to this when you add a column here and start using it. The full Vergilant
// service keeps the same tables under an ordered migration runner, where the fix
// is to run the migrations rather than apply schema.sql over them.
var requiredColumns = map[string][]string{
	"projects": {"key", "monthly_request_limit", "user_id"},
	// Read by projectStatus. Without it the proxy cannot tell a first offence
	// from a repeat one, and hands back the grace window every month.
	"users": {"id", "consecutive_cap_months"},
	"requests": {
		"project_key", "timestamp", "provider", "model", "status",
		"latency_ms", "validate_ms", "upstream_ms", "first_token_ms",
		"input_tokens", "output_tokens", "estimated_cost_usd", "error",
	},
}

// Reports every missing column in one error, rather than one per re-run.
func checkSchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = ANY($1)`,
		[]string{"projects", "requests", "users"})
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
		// Stable message: Go randomizes map order.
		slices.Sort(missing)
		return fmt.Errorf("database is missing %s; apply the proxy's schema.sql",
			strings.Join(missing, ", "))
	}
	return nil
}
