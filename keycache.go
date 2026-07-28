package main

import (
	"sync"
	"time"
)

// A brief in-memory cache of positive key validations, so repeat traffic from
// a known project skips the cross-region projectStatus round-trip in db.go.
//
// Deliberate tradeoffs, accepted while there are no real users or attackers:
//
//   - Only POSITIVE results are cached. Caching misses would let a flood of
//     random keys grow the map without bound; positives are bounded by the
//     number of real projects (same reasoning as the rate limiter's buckets),
//     so no reaper is needed.
//   - A revoked or deleted key stays usable for up to keyCacheTTL (revocation
//     lag). Kept short on purpose.
//   - The cached monthly count goes stale for up to keyCacheTTL, widening the
//     soft-quota overshoot the handler already tolerates.
//   - The cached limit goes stale the same way, so a project whose cap was just
//     raised keeps hitting the old one for up to keyCacheTTL. On hosted
//     Vergilant that's the moment someone pays, which is the worst moment for
//     it - but it only bites a project already sitting at its cap, and there's
//     no invalidation channel from the API to this process. Accepted, not fixed.
const keyCacheTTL = 45 * time.Second

// keyCacheMaxStale is how far past expiry an entry may still be served when the
// database cannot be reached at all - see getStale and its use in main.go.
//
// It exists so an outage cannot extend the tradeoffs above without bound. A key
// revoked during a long outage stays usable for this long, and a project at its
// cap can overshoot for this long, so it is deliberately minutes rather than
// hours: long enough to ride out the database blips that actually happen, short
// enough that the accepted lag stays in the same order of magnitude.
const keyCacheMaxStale = 15 * time.Minute

// limit is already resolved: the caller has substituted monthlyLimit for a NULL
// column, so nothing downstream has to know the difference.
type cachedStatus struct {
	monthCount int
	limit      int
	// capMonths is the owner's consecutive-months-over-cap count, cached
	// alongside the rest because the quota ladder needs all three together and
	// they come from one query.
	capMonths int
	expires   time.Time
}

type keyCache struct {
	mu      sync.Mutex
	entries map[string]cachedStatus
}

func newKeyCache() *keyCache {
	return &keyCache{entries: make(map[string]cachedStatus)}
}

// get returns the cached month count and request limit for a known-valid key,
// or ok=false if there is no live entry (never cached, or expired).
func (c *keyCache) get(key string) (monthCount, limit, capMonths int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires) {
		return 0, 0, 0, false
	}
	return e.monthCount, e.limit, e.capMonths, true
}

// getStale returns the last known-good answer for a key whose entry has expired
// but is not yet older than keyCacheMaxStale. It is the fallback for one
// situation only: the database is unreachable, so there is no way to ask.
//
// Expired entries are already left in the map - get simply refuses to return
// them - so this reads what is there rather than keeping anything alive longer.
// The map stays bounded by the number of real projects for the reason at the
// top of this file: only positive results are ever cached.
//
// Callers must not use this in place of a failed lookup generally. An unknown
// key has no entry here, and must keep being rejected.
func (c *keyCache) getStale(key string) (monthCount, limit, capMonths int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires.Add(keyCacheMaxStale)) {
		return 0, 0, 0, false
	}
	return e.monthCount, e.limit, e.capMonths, true
}

// put records a positive validation. Only ever called for keys that exist, so
// the map stays bounded by the count of real projects.
func (c *keyCache) put(key string, monthCount, limit, capMonths int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cachedStatus{
		monthCount: monthCount, limit: limit, capMonths: capMonths,
		expires: time.Now().Add(keyCacheTTL),
	}
}
