package main

import (
	"sync"
	"time"
)

// Rate-limiting has two independent layers, doing two different jobs:
//
//  1. The monthly per-project cap lives in Postgres, counted in db.go. It's a
//     durable quota that MUST survive a restart, so it can't be in memory.
//     See monthlyLimit / projectStatus.
//  2. The per-project token bucket below is an in-memory abuse guardrail: it
//     stops a single project (e.g. a runaway agent looping thousands of times
//     a second) from hammering the database and the upstream provider. Losing
//     this state on restart is harmless, so in-memory is the right home.
//
// These are guardrails against pathological bursts, NOT the customer-facing
// usage limit, and they're set generously so normal bursty traffic sails
// through.
const (
	burstSize       = 30 // max requests a project can fire back-to-back
	refillPerSecond = 10 // sustained requests/second once the burst is spent
)

// monthlyLimit is the durable per-project cap enforced against the Postgres
// month count (see projectStatus in db.go). Set MONTHLY_REQUEST_LIMIT to
// change it, or to 0 to disable the cap entirely; main sets it at startup.
var monthlyLimit int = 10000

// bucket is one project's token balance. tokens is fractional so a partial
// refill between requests isn't rounded away.
type bucket struct {
	tokens float64
	last   time.Time
}

// limiter holds one bucket per project key. The map only ever gets entries for
// keys that already passed validation (allow is called after projectStatus),
// so it's bounded by the number of real projects. No reaper needed, and a
// flood of garbage keys can't grow it.
type limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens added per second
	burst   float64 // ceiling on accumulated tokens
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
	}
}

// allow reports whether the project may make one more request right now,
// spending a token if so.
func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b := l.buckets[key]
	if b == nil {
		// Start full so a fresh project isn't throttled before it's done anything.
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
