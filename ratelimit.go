package main

import (
	"github.com/arajenk/vergilant-proxy/quota"

	"sync"
	"time"
)

// Two independent layers limit a project. The monthly cap is durable and lives
// in Postgres (see monthlyLimit and projectStatus). The token bucket below is an
// in-memory guardrail against pathological bursts, like a runaway agent looping
// thousands of times a second; losing it on restart is harmless. It is not the
// customer-facing limit, so it is set generously.
const (
	burstSize       = 30
	refillPerSecond = 10
)

// Set from MONTHLY_REQUEST_LIMIT at startup, 0 to disable. This is the default
// for projects with no monthly_request_limit of their own.
var monthlyLimit int = quota.DefaultFreeMonthlyLimit

// Fractional tokens, so a partial refill between requests is not rounded away.
type bucket struct {
	tokens float64
	last   time.Time
}

// Only holds keys that passed validation, since allow runs after projectStatus,
// so it is bounded by the number of real projects and needs no reaper.
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

// allow reports whether the project may make one more request, spending a token
// if so.
func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b := l.buckets[key]
	if b == nil {
		// Start full, so a new project is not throttled immediately.
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
