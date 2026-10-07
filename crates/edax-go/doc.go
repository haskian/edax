// Package edax provides command dispatch, event sourcing, sagas and message delivery.
// Storage and transport adapters live under backend; applications can implement the
// same store interfaces for their own infrastructure.
//
// The shared Rust kernels own encoding, ordering, tenancy, idempotency, retry and
// query decisions. Native failures return *Error; errors.As exposes the code and
// message, while errors.Is compares category sentinels.
//
// Close SDK components that own native resources after their operations finish.
// Automatic cleanup is a fallback with no timing guarantee. Native calls and
// Close are synchronized so goroutines cannot use freed native allocations.
package edax
