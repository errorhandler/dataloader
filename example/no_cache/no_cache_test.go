package no_cache_test

import (
	"context"
	"fmt"

	dataloader "github.com/errorhandler/dataloader"
)

func ExampleNoCache() {
	// go-cache will automaticlly cleanup expired items on given diration
	cache := &dataloader.NoCache[string, string]{}
	loader := dataloader.NewBatchedLoader(batchFunc, dataloader.WithCache[string, string](cache))

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
