package dataloader

import "context"

// The Cache interface. If a custom cache is provided, it must implement this interface.
type Cache[Key any, Value any] interface {
	Get(context.Context, Key) (Thunk[Value], bool)
	Set(context.Context, Key, Thunk[Value])
	Delete(context.Context, Key) bool
	Clear()
}

// NoCache implements Cache interface where all methods are noops.
// This is useful for when you don't want to cache items but still
// want to use a data loader
type NoCache[Key any, Value any] struct{}

// Get is a NOOP
func (c *NoCache[Key, Value]) Get(context.Context, Key) (Thunk[Value], bool) { return nil, false }

// Set is a NOOP
func (c *NoCache[Key, Value]) Set(context.Context, Key, Thunk[Value]) { return }

// Delete is a NOOP
func (c *NoCache[Key, Value]) Delete(context.Context, Key) bool { return false }

// Clear is a NOOP
func (c *NoCache[Key, Value]) Clear() { return }
