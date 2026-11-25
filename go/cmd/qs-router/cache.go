package main

import (
	"sync"
	"time"
)

// decisionCache remembers the answer for a statement, for a while.
//
// The key carries the database, the role and search_path the statement was
// planned under, and the routing mode, because the same text names different
// tables under a different search_path, and a decision reused across that
// difference would put the mirror schema in front of a table it does not
// mirror. See session.schemasKey for how that is kept honest on a cache hit.
type decisionCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	entries map[string]cacheEntry
	now     func() time.Time
}

type cacheEntry struct {
	d       Decision
	expires time.Time
}

func newDecisionCache(ttl time.Duration, max int) *decisionCache {
	return &decisionCache{ttl: ttl, max: max, entries: map[string]cacheEntry{}, now: time.Now}
}

func (c *decisionCache) get(key string) (Decision, bool) {
	if c.ttl <= 0 {
		return Decision{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return Decision{}, false
	}
	if !c.now().Before(e.expires) {
		delete(c.entries, key)
		return Decision{}, false
	}
	return e.d, true
}

func (c *decisionCache) put(key string, d Decision) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		// Exact-text keys are unbounded (every literal is a new one). Dropping
		// everything is crude and cannot route anything wrongly.
		c.entries = map[string]cacheEntry{}
	}
	c.entries[key] = cacheEntry{d: d, expires: c.now().Add(c.ttl)}
}

// catalogCache holds, per database, which tables are mirrored and which
// indexes are unique. It is filled through a client's own connection (the
// router has no credentials of its own) and shared by all of them.
type catalogCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]catalogEntry
}

type catalogEntry struct {
	cat     *Catalog
	expires time.Time
}

func (c *catalogCache) get(db string) *Catalog {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[db]
	if !ok || !time.Now().Before(e.expires) {
		return nil
	}
	return e.cat
}

func (c *catalogCache) put(db string, cat *Catalog) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[db] = catalogEntry{cat: cat, expires: time.Now().Add(c.ttl)}
}
