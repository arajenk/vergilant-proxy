package main

import (
	"github.com/arajenk/vergilant-proxy/quota"

	"sync"
	"time"
)

// Two different limits. The monthly cap is the real one and lives in Postgres.
// The bucket below just catches silly bursts, like an agent looping thousands of
// times a second. In memory, and losing it on restart is fine.
const (
	burstSize       = 30
	refillPerSecond = 10
)

// Set from MONTHLY_REQUEST_LIMIT at startup, 0 turns the cap off. This is the
// default for projects with no limit of their own.
var monthlyLimit int = quota.DefaultFreeMonthlyLimit

// Fractional tokens so a partial refill between requests isn't rounded away.
type bucket struct {
	tokens float64
	last   time.Time
}

// Only holds keys that already passed validation, since allow runs after
// projectStatus. So it's bounded by the number of real projects and nothing
// needs to clean it up.
type limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens added per second
	burst   float64 // most tokens you can bank
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
	}
}

// allow says whether the project can make one more request, and spends a token
// if it can.
func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b := l.buckets[key]
	if b == nil {
		// Start full so a new project isn't throttled straight away.
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
