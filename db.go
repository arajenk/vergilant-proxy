package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// USD per million tokens, the way providers publish it.
type modelPrice struct {
	InputPerMillion  float64
	OutputPerMillion float64
}

// Kept by hand. A model that isn't in here costs $0, see estimatedCost. Claude
// rows checked against Anthropic's pricing page on 2026-07-28, Sonnet 5 on intro
// pricing until 2026-08-31. The OpenAI rows haven't been checked.
var priceMap = map[string]modelPrice{
	"claude-fable-5":            {InputPerMillion: 10, OutputPerMillion: 50},
	"claude-opus-5":             {InputPerMillion: 5, OutputPerMillion: 25},
	"claude-opus-4-8":           {InputPerMillion: 5, OutputPerMillion: 25},
	"claude-sonnet-5":           {InputPerMillion: 2, OutputPerMillion: 10},
	"claude-haiku-4-5":          {InputPerMillion: 1, OutputPerMillion: 5},
	"claude-haiku-4-5-20251001": {InputPerMillion: 1, OutputPerMillion: 5},

	"gpt-4o":       {InputPerMillion: 2.5, OutputPerMillion: 10},
	"gpt-4o-mini":  {InputPerMillion: 0.15, OutputPerMillion: 0.6},
	"gpt-4.1":      {InputPerMillion: 2, OutputPerMillion: 8},
	"gpt-4.1-mini": {InputPerMillion: 0.4, OutputPerMillion: 1.6},
	"o1":           {InputPerMillion: 15, OutputPerMillion: 60},
	"o3-mini":      {InputPerMillion: 1.1, OutputPerMillion: 4.4},
}

// Models we've already warned about, so each one logs once instead of on every
// request.
var (
	unpricedMu   sync.Mutex
	unpricedSeen = map[string]bool{}
)

// Logs the model name and nothing else, which is metadata like everything else
// we keep.
func warnUnpricedOnce(model string) {
	unpricedMu.Lock()
	defer unpricedMu.Unlock()
	if unpricedSeen[model] {
		return
	}
	unpricedSeen[model] = true
	slog.Warn("unpriced model: not in priceMap, so its cost is recorded as $0 "+
		"and cost_spike cannot fire for any project using it; add it to priceMap in db.go",
		"model", model)
}

func estimatedCost(model string, inputTokens, outputTokens int) float64 {
	price, ok := priceMap[model]
	if !ok {
		// Zero instead of a guess, because a made up price poisons every cost
		// number after it. Zero isn't harmless either: the cost alert compares
		// spend to a baseline, and 0 never beats 5x0. That's what the warning is
		// for.
		warnUnpricedOnce(model)
		return 0
	}
	return float64(inputTokens)/1_000_000*price.InputPerMillion +
		float64(outputTokens)/1_000_000*price.OutputPerMillion
}

type requestRecord struct {
	ProjectKey string
	Timestamp  time.Time
	Provider   string
	Model      string
	Status     int
	LatencyMs  int64
	// LatencyMs split up: the key lookup, and the provider's own time. Pointers
	// because a request that died before either step has no number to give, and
	// a 0 would read as "took no time" instead of "never measured".
	ValidateMs       *int64
	UpstreamMs       *int64
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
	// pgxpool.New doesn't actually connect, so ping here to fail at startup
	// instead of on someone's first request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// One round trip on the hot path. The month count comes off the requests table
// rather than a counter, so there's nothing to keep in sync, and the
// (project_key, timestamp) index makes it cheap.
//
// Unknown key gives pgx.ErrNoRows. limit is nil when the column is NULL, meaning
// use monthlyLimit. LEFT JOIN because projects.user_id can be NULL.
func projectStatus(ctx context.Context, pool *pgxpool.Pool, key string) (limit *int, monthCount, capMonths int, err error) {
	err = pool.QueryRow(ctx, `
		SELECT
			p.monthly_request_limit,
			(SELECT count(*) FROM requests
			   WHERE project_key = p.key AND timestamp >= date_trunc('month', now())),
			COALESCE(u.consecutive_cap_months, 0)
		FROM projects p
		LEFT JOIN users u ON u.id = p.user_id
		WHERE p.key = $1`,
		key,
	).Scan(&limit, &monthCount, &capMonths)
	return limit, monthCount, capMonths, err
}

func saveRequest(ctx context.Context, pool *pgxpool.Pool, rec requestRecord) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO requests
			(project_key, timestamp, provider, model, status, latency_ms, validate_ms, upstream_ms, first_token_ms, input_tokens, output_tokens, estimated_cost_usd, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		rec.ProjectKey, rec.Timestamp, rec.Provider, rec.Model, rec.Status, rec.LatencyMs,
		rec.ValidateMs, rec.UpstreamMs,
		rec.FirstTokenMs, rec.InputTokens, rec.OutputTokens, rec.EstimatedCostUSD, rec.Error,
	)
	return err
}
