package main

import (
	"context"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// modelPrice is USD per million tokens, matching how Anthropic (and the
// rest of the industry) publishes pricing.
type modelPrice struct {
	InputPerMillion  float64
	OutputPerMillion float64
}

// Hardcoded, updated by hand. Sonnet 5 is at introductory pricing
// ($2/$10 per million) through 2026-08-31; it reverts to $3/$15 after.
//
// The OpenAI rows below are current as of this author's knowledge cutoff
// (January 2026), not verified against OpenAI's live pricing page — check
// https://openai.com/api/pricing before relying on them for real billing.
var priceMap = map[string]modelPrice{
	"claude-opus-4-8":           {InputPerMillion: 5, OutputPerMillion: 25},
	"claude-sonnet-5":           {InputPerMillion: 2, OutputPerMillion: 10},
	"claude-haiku-4-5-20251001": {InputPerMillion: 1, OutputPerMillion: 5},

	"gpt-4o":       {InputPerMillion: 2.5, OutputPerMillion: 10},
	"gpt-4o-mini":  {InputPerMillion: 0.15, OutputPerMillion: 0.6},
	"gpt-4.1":      {InputPerMillion: 2, OutputPerMillion: 8},
	"gpt-4.1-mini": {InputPerMillion: 0.4, OutputPerMillion: 1.6},
	"o1":           {InputPerMillion: 15, OutputPerMillion: 60},
	"o3-mini":      {InputPerMillion: 1.1, OutputPerMillion: 4.4},
}

func estimatedCost(model string, inputTokens, outputTokens int) float64 {
	price, ok := priceMap[model]
	if !ok {
		return 0
	}
	return float64(inputTokens)/1_000_000*price.InputPerMillion +
		float64(outputTokens)/1_000_000*price.OutputPerMillion
}

type requestRecord struct {
	ProjectKey       string
	Timestamp        time.Time
	Provider         string
	Model            string
	Status           int
	LatencyMs        int64
	FirstTokenMs     *int64
	InputTokens      int
	OutputTokens     int
	EstimatedCostUSD float64
	Error            *string
}

func connectDB(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, err
	}
	// pgxpool.New doesn't connect eagerly, so ping now to fail at startup
	// instead of on the first proxied request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// projectStatus answers the two things the proxy needs before forwarding, in a
// single round-trip on the hot path: does this key exist, and how many requests
// has it already made this calendar month (for the durable free-tier cap).
//
// The month count is derived from the requests table itself — the single source
// of truth — rather than a separate counter, so there's nothing to keep in sync.
// The (project_key, timestamp) index in schema.sql keeps this a cheap range
// count. now() is UTC in Postgres, so date_trunc gives a UTC month boundary.
func projectStatus(ctx context.Context, pool *pgxpool.Pool, key string) (exists bool, monthCount int, err error) {
	err = pool.QueryRow(ctx, `
		SELECT
			EXISTS(SELECT 1 FROM projects WHERE key = $1),
			(SELECT count(*) FROM requests
			   WHERE project_key = $1 AND timestamp >= date_trunc('month', now()))`,
		key,
	).Scan(&exists, &monthCount)
	return exists, monthCount, err
}

func saveRequest(ctx context.Context, pool *pgxpool.Pool, rec requestRecord) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO requests
			(project_key, timestamp, provider, model, status, latency_ms, first_token_ms, input_tokens, output_tokens, estimated_cost_usd, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		rec.ProjectKey, rec.Timestamp, rec.Provider, rec.Model, rec.Status, rec.LatencyMs,
		rec.FirstTokenMs, rec.InputTokens, rec.OutputTokens, rec.EstimatedCostUSD, rec.Error,
	)
	return err
}
