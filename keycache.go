package main

import (
	"sync"
	"time"
)

// Caches keys we've already checked, so repeat traffic skips the cross region
// lookup in db.go. Only keys that validated go in, so junk keys can't grow the
// map.
//
// Inside the TTL a revoked key still works and the counts go stale, which is why
// it's short. Nothing tells this process when a key changes.
const keyCacheTTL = 45 * time.Second

// How far past expiry we'll still serve an entry when the database can't be
// reached at all. Minutes, not hours: long enough for the blips that actually
// happen, short enough that a revoked key doesn't outlive the outage by much.
const keyCacheMaxStale = 15 * time.Minute

// limit is already resolved here. The caller swaps in monthlyLimit when the
// column is NULL.
type cachedStatus struct {
	monthCount int
	limit      int
	// How many months in a row the owner has been over cap. Cached with the rest
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

// get returns the cached status for a key we know is valid, or ok=false if
// there's no live entry.
func (c *keyCache) get(key string) (monthCount, limit, capMonths int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires) {
		return 0, 0, 0, false
	}
	return e.monthCount, e.limit, e.capMonths, true
}

// getStale returns the last good answer for a key that expired less than
// keyCacheMaxStale ago. Only for when the database is unreachable and there's no
// way to ask. A key we've never seen has no entry, so it still gets rejected.
func (c *keyCache) getStale(key string) (monthCount, limit, capMonths int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || time.Now().After(e.expires.Add(keyCacheMaxStale)) {
		return 0, 0, 0, false
	}
	return e.monthCount, e.limit, e.capMonths, true
}

// put saves a key that checked out. Only called for keys that exist.
func (c *keyCache) put(key string, monthCount, limit, capMonths int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cachedStatus{
		monthCount: monthCount, limit: limit, capMonths: capMonths,
		expires: time.Now().Add(keyCacheTTL),
	}
}
