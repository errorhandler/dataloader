package no_cache_test

import (
	"context"
	"fmt"

	dataloader "github.com/errorhandler/dataloader"
)

func ExampleNoCache() {
	// go-cache will automaticlly cleanup expired items on given diration
	cache := &dataloader.NoCache{}
	loader := dataloader.NewBatchedLoader(batchFunc, dataloader.WithCache(cache))

	result, err := loader.Load(context.TODO(), dataloader.StringKey("some key"))()
	if err != nil {
		// handle error
	}

	fmt.Printf("identity: %s", result)
	// Output: identity: some key
}

func batchFunc(_ context.Context, keys dataloader.Keys) []*dataloader.Result {
	var results []*dataloader.Result
	// do some pretend work to resolve keys
	for _, key := range keys {
		results = append(results, &dataloader.Result{Data: key.String()})
	}
	return results
}
