package lru_cache_test

// This is an exmaple of using go-cache as a long term cache solution for
// dataloader.

import (
	"context"
	"fmt"

	dataloader "github.com/errorhandler/dataloader"
	lru "github.com/errorhandler/golang-lru"
)

// Cache implements the dataloader.Cache interface
type cache[Key, Value any] struct {
	*lru.ARCCache[Key, dataloader.Thunk[Value]]
}

// Get gets an item from the cache
func (c *cache[Key, Value]) Get(_ context.Context, key Key) (dataloader.Thunk[Value], bool) {
	v, ok := c.ARCCache.Get(key)
	if ok {
		return v, ok
	}
	return nil, ok
}

// Set sets an item in the cache
func (c *cache[Key, Value]) Set(_ context.Context, key Key, value dataloader.Thunk[Value]) {
	c.ARCCache.Add(key, value)
}

// Delete deletes an item in the cache
func (c *cache[Key, Value]) Delete(_ context.Context, key Key) bool {
	if c.ARCCache.Contains(key) {
		c.ARCCache.Remove(key)
		return true
	}
	return false
}

// Clear cleasrs the cache
func (c *cache[Key, Value]) Clear() {
	c.ARCCache.Purge()
}

func ExampleGolangLRU() {
	// go-cache will automaticlly cleanup expired items on given duration.
	c, _ := lru.NewARC[string, dataloader.Thunk[string]](100)
	cache := &cache[string, string]{ARCCache: c}
	loader := dataloader.NewBatchedLoader(batchFunc, dataloader.WithCache[string, string](cache))

	// immediately call the future function from loader
	result, err := loader.Load(context.TODO(), "some key")()
	if err != nil {
		// handle error
	}

	fmt.Printf("identity: %s", result)
	// Output: identity: some key
}

func batchFunc(_ context.Context, keys []string) []*dataloader.Result[string] {
	var results []*dataloader.Result[string]
	// do some pretend work to resolve keys
	for _, key := range keys {
		results = append(results, &dataloader.Result[string]{Data: key})
	}
	return results
}
