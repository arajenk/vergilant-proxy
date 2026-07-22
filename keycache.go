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
//     it — but it only bites a project already sitting at its cap, and there's
//     no invalidation channel from the API to this process. Accepted, not fixed.
const keyCacheTTL = 45 * time.Second

// limit is already resolved: the caller has substituted monthlyLimit for a NULL
// column, so nothing downstream has to know the difference.
type cachedStatus struct {
	monthCount int
	limit      int
	expires    time.Time
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
func (c *keyCache) get(key string) (monthCount, limit int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires) {
		return 0, 0, false
	}
	return e.monthCount, e.limit, true
}

// put records a positive validation. Only ever called for keys that exist, so
// the map stays bounded by the count of real projects.
func (c *keyCache) put(key string, monthCount, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cachedStatus{monthCount: monthCount, limit: limit, expires: time.Now().Add(keyCacheTTL)}
}
