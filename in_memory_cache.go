package dataloader

import (
	"context"
	"sync"
)

// InMemoryCache is an in memory implementation of Cache interface.
// This simple implementation is well suited for
// a "per-request" dataloader (i.e. one that only lives
// for the life of an http request) but it's not well suited
// for long lived cached items.
type InMemoryCache[Key comparable, Value any] struct {
	items map[Key]Thunk[Value]
	mu    sync.RWMutex
}

// NewCache constructs a new InMemoryCache
func NewCache[Key comparable, Value any]() *InMemoryCache[Key, Value] {
	items := make(map[Key]Thunk[Value])
	return &InMemoryCache[Key, Value]{
		items: items,
	}
}

// Set sets the `value` at `key` in the cache
func (c *InMemoryCache[Key, Value]) Set(_ context.Context, key Key, value Thunk[Value]) {
	c.mu.Lock()
	c.items[key] = value
	c.mu.Unlock()
}

// Get gets the value at `key` if it exsits, returns value (or nil) and bool
// indicating of value was found
func (c *InMemoryCache[Key, Value]) Get(_ context.Context, key Key) (Thunk[Value], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found {
		return nil, false
	}

	return item, true
}

// Delete deletes item at `key` from cache
func (c *InMemoryCache[Key, Value]) Delete(ctx context.Context, key Key) bool {
	if _, found := c.Get(ctx, key); found {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.items, key)
		return true
	}
	return false
}

// Clear clears the entire cache
func (c *InMemoryCache[Key, Value]) Clear() {
	c.mu.Lock()
	c.items = map[Key]Thunk[Value]{}
	c.mu.Unlock()
}
