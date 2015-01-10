// Shamelessly copied and adapted from:
// https://github.com/camlistore/camlistore/blob/master/pkg/lru/cache.go
// Copyright 2011 Google Inc. - http://www.apache.org/licenses/LICENSE-2.0
package cluster

import (
	"container/list"

	"gopkg.in/redis.v2"
)

// connLRU is an connection LRU cache
// This version is not thread-safe!
type connLRU struct {
	maxEntries int
	ll         *list.List
	cache      map[string]*list.Element
}

type cachedConn struct {
	addr string
	conn *redis.Client
}

func newLRU(maxEntries int) *connLRU {
	return &connLRU{
		maxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
	}
}

// Add adds the provided addr and conn to the cache, evicting
// an old item if necessary.
func (c *connLRU) Add(addr string, conn *redis.Client) {
	if ee, ok := c.cache[addr]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*cachedConn).conn = conn
		return
	}

	// Add to cache if not present
	ele := c.ll.PushFront(&cachedConn{addr, conn})
	c.cache[addr] = ele

	if c.ll.Len() > c.maxEntries {
		c.RemoveOldest()
	}
}

// Fetch gets or creates a new connection
func (c *connLRU) Fetch(addr string, newConn func(string) *redis.Client) *redis.Client {
	conn, ok := c.Get(addr)
	if !ok {
		conn = newConn(addr)
		c.Add(addr, conn)
	}
	return conn
}

// Get fetches the addr's conn from the cache.
// The ok result will be true if the item was found.
func (c *connLRU) Get(addr string) (conn *redis.Client, ok bool) {
	if ele, hit := c.cache[addr]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*cachedConn).conn, true
	}
	return
}

// RemoveOldest removes the oldest item in the cache.
// If the cache is empty, the empty string and nil are returned.
func (c *connLRU) RemoveOldest() {
	ele := c.ll.Back()
	if ele == nil {
		return
	}

	c.ll.Remove(ele)
	ent := ele.Value.(*cachedConn)
	delete(c.cache, ent.addr)

	// Close connection
	ent.conn.Close()
}

// Clear clears the cache
func (c *connLRU) Clear() {
	size := c.Len()
	for i := 0; i < size; i++ {
		c.RemoveOldest()
	}
}

// Len returns the number of items in the cache.
func (c *connLRU) Len() int {
	return c.ll.Len()
}
