package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// USD per million tokens, matching how providers publish pricing.
type modelPrice struct {
	InputPerMillion  float64
	OutputPerMillion float64
}

// Maintained by hand. A missing model costs $0; see estimatedCost.
//
// Claude rows verified against platform.claude.com/docs/en/about-claude/pricing
// on 2026-07-28. Sonnet 5 is on introductory pricing until 2026-08-31, then
// reverts to $3/$15. The OpenAI rows are unverified.
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

// Models already warned about, so each produces one log line rather than one
// per request.
var (
	unpricedMu   sync.Mutex
	unpricedSeen = map[string]bool{}
)

// Logs the model name only, which is metadata like every other field recorded.
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
		// Zero rather than a guess, since an invented price corrupts every cost
		// number downstream. Zero is not harmless either: cost_spike compares
		// spend against a baseline, and 0 never exceeds 5x0. Hence the warning.
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
	// The split of LatencyMs: the key lookup, and the provider's time. Pointers
	// because a request that failed before either stage has no number to report,
	// and a zero would read as "added nothing" rather than "not measured".
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
	// pgxpool.New does not connect eagerly, so ping to fail at startup rather
	// than on the first proxied request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// One round-trip on the hot path. The month count comes from the requests table
// rather than a counter, so there is nothing to keep in sync; the
// (project_key, timestamp) index keeps it a cheap range count. Postgres now() is
// UTC, so date_trunc gives a UTC month boundary.
//
// An unknown key returns pgx.ErrNoRows. limit is nil for a NULL column, meaning
// "use monthlyLimit", which the caller resolves. capMonths is the owner's
// consecutive whole months over cap, which the quota package reads.
//
// LEFT JOIN and COALESCE because projects.user_id is nullable: an unowned
// project has no history, same as a clean one.
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
