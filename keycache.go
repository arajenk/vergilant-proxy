package main

import (
	"sync"
	"time"
)

// Cache of positive key validations, so repeat traffic from a known project
// skips the cross-region projectStatus round-trip in db.go. Only positive
// results are cached, which bounds the map by the number of real projects and
// keeps a flood of random keys from growing it.
//
// Within the TTL: a revoked key stays usable, and the count and limit go stale.
// Kept short for that reason. There is no invalidation channel from the API to
// this process.
const keyCacheTTL = 45 * time.Second

// How far past expiry an entry may still be served when the database cannot be
// reached at all. Minutes rather than hours: long enough for the blips that
// actually happen, short enough that a revoked key does not outlive the outage
// by much.
const keyCacheMaxStale = 15 * time.Minute

// limit is already resolved: the caller substitutes monthlyLimit for a NULL
// column.
type cachedStatus struct {
	monthCount int
	limit      int
	// The owner's consecutive-months-over-cap count. Cached with the rest
	// because the quota ladder needs all three and one query returns them.
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

// get returns the cached status for a known-valid key, or ok=false if there is
// no live entry.
func (c *keyCache) get(key string) (monthCount, limit, capMonths int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires) {
		return 0, 0, 0, false
	}
	return e.monthCount, e.limit, e.capMonths, true
}

// getStale returns the last known-good answer for a key whose entry expired
// less than keyCacheMaxStale ago. For one situation only: the database is
// unreachable, so there is no way to ask. An unknown key has no entry here and
// must keep being rejected.
func (c *keyCache) getStale(key string) (monthCount, limit, capMonths int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires.Add(keyCacheMaxStale)) {
		return 0, 0, 0, false
	}
	return e.monthCount, e.limit, e.capMonths, true
}

// put records a positive validation. Only called for keys that exist.
func (c *keyCache) put(key string, monthCount, limit, capMonths int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cachedStatus{
		monthCount: monthCount, limit: limit, capMonths: capMonths,
		expires: time.Now().Add(keyCacheTTL),
	}
}
