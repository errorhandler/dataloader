package dataloader

import (
	"context"
)

type (
	TraceLoadFinishFunc[Value any]     func(Thunk[Value])
	TraceLoadManyFinishFunc[Value any] func(ThunkMany[Value])
	TraceBatchFinishFunc[Value any]    func([]*Result[Value])
)

// Tracer is an interface that may be used to implement tracing.
type Tracer[Key any, Value any] interface {
	// TraceLoad will trace the calls to Load
	TraceLoad(ctx context.Context, key Key) (context.Context, TraceLoadFinishFunc[Value])
	// TraceLoadMany will trace the calls to LoadMany
	TraceLoadMany(ctx context.Context, keys []Key) (context.Context, TraceLoadManyFinishFunc[Value])
	// TraceBatch will trace data loader batches
	TraceBatch(ctx context.Context, keys []Key) (context.Context, TraceBatchFinishFunc[Value])
}

// NoopTracer is the default (noop) tracer
type NoopTracer[Key any, Value any] struct{}

// TraceLoad is a noop function
func (NoopTracer[Key, Value]) TraceLoad(ctx context.Context, key Key) (context.Context, TraceLoadFinishFunc[Value]) {
	return ctx, func(Thunk[Value]) {}
}

// TraceLoadMany is a noop function
func (NoopTracer[Key, Value]) TraceLoadMany(ctx context.Context, keys []Key) (context.Context, TraceLoadManyFinishFunc[Value]) {
	return ctx, func(ThunkMany[Value]) {}
}

// TraceBatch is a noop function
func (NoopTracer[Key, Value]) TraceBatch(ctx context.Context, keys []Key) (context.Context, TraceBatchFinishFunc[Value]) {
	return ctx, func(result []*Result[Value]) {}
}
