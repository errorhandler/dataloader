// This is an exmaple of using go-cache as a long term cache solution for
// dataloader.
package ttl_cache_test

import (
	"context"
	"fmt"
	"time"

	dataloader "github.com/errorhandler/dataloader"
	cache "github.com/patrickmn/go-cache"
)

// Cache implements the dataloader.Cache interface
type Cache[Key ~string, Value any] struct {
	c *cache.Cache
}

// Get gets a value from the cache
func (c *Cache[Key, Value]) Get(_ context.Context, key Key) (dataloader.Thunk[Value], bool) {
	v, ok := c.c.Get(string(key))
	if ok {
		return v.(dataloader.Thunk[Value]), ok
	}
	return nil, ok
}

// Set sets a value in the cache
func (c *Cache[Key, Value]) Set(_ context.Context, key Key, value dataloader.Thunk[Value]) {
	c.c.Set(string(key), value, 0)
}

// Delete deletes and item in the cache
func (c *Cache[Key, Value]) Delete(_ context.Context, key Key) bool {
	if _, found := c.c.Get(string(key)); found {
		c.c.Delete(string(key))
		return true
	}
	return false
}

// Clear clears the cache
func (c *Cache[Key, Value]) Clear() {
	c.c.Flush()
}

func ExampleTTLCache() {
	// go-cache will automaticlly cleanup expired items on given diration
	c := cache.New(15*time.Minute, 15*time.Minute)
	cache := &Cache[string, string]{c}
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
