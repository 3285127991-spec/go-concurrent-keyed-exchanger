// Package keyedexchanger provides a concurrency-safe exchanger that pairs
// callers by string key: the first Exchange call for a key blocks until a
// second call with the same key arrives, then both return each other's value.
package keyedexchanger

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrEmptyKey is returned by Exchange when key is empty.
	ErrEmptyKey = errors.New("keyedexchanger: key must not be empty")
	// ErrClosed is returned by Exchange when the exchanger is closed, and is
	// delivered to callers still waiting for a partner when Close is called.
	ErrClosed = errors.New("keyedexchanger: exchanger is closed")
)

// result is the outcome delivered to a waiting caller.
type result struct {
	value any
	err   error
}

// waiter represents one unmatched Exchange call. Each call gets its own
// waiter instance, so rounds (generations) of the same key never mix.
type waiter struct {
	value any
	ch    chan result // buffered with capacity 1; receives exactly one result
}

// Exchanger pairs concurrent Exchange calls by key. The zero value is not
// usable; create one with New.
type Exchanger struct {
	mu      sync.Mutex
	closed  bool
	waiting map[string]*waiter
}

// New returns an Exchanger ready for use.
func New() *Exchanger {
	return &Exchanger{waiting: make(map[string]*waiter)}
}

// Exchange blocks until another goroutine calls Exchange with the same key,
// then returns the value that the other caller passed in (and delivers value to
// it). The first call for a key waits; the second pairs with it atomically,
// after which the key is free for a new, independent round.
//
// If ctx is cancelled while waiting, only this call is removed and ctx.Err()
// is returned. If cancellation races with an arriving partner, exactly one
// outcome stands: the pair completes and both callers return values, or the
// cancellation wins and the partner becomes the new waiter.
//
// Exchange returns ErrEmptyKey for an empty key and ErrClosed after Close.
func (e *Exchanger) Exchange(ctx context.Context, key string, value any) (any, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	if w, ok := e.waiting[key]; ok {
		// Pair with the current waiter: hand it our value, take its value.
		delete(e.waiting, key)
		w.ch <- result{value: value}
		e.mu.Unlock()
		return w.value, nil
	}
	w := &waiter{value: value, ch: make(chan result, 1)}
	e.waiting[key] = w
	e.mu.Unlock()

	select {
	case r := <-w.ch:
		return r.value, r.err
	case <-ctx.Done():
		e.mu.Lock()
		if e.waiting[key] == w {
			// Still waiting: cancel succeeds and only this call is removed.
			delete(e.waiting, key)
			e.mu.Unlock()
			return nil, ctx.Err()
		}
		e.mu.Unlock()
		// A partner or Close already claimed this waiter under the lock and
		// deposited a result in the buffered channel, so this never blocks.
		r := <-w.ch
		return r.value, r.err
	}
}

// Snapshot returns the keys currently waiting for a partner, sorted in
// lexicographic order.
func (e *Exchanger) Snapshot() []string {
	e.mu.Lock()
	keys := make([]string, 0, len(e.waiting))
	for k := range e.waiting {
		keys = append(keys, k)
	}
	e.mu.Unlock()
	sort.Strings(keys)
	return keys
}

// Len returns the number of keys currently waiting for a partner.
func (e *Exchanger) Len() int {
	e.mu.Lock()
	n := len(e.waiting)
	e.mu.Unlock()
	return n
}

// Close wakes every waiting caller with ErrClosed and rejects all future
// Exchange calls with ErrClosed. Calls that already paired are unaffected.
// Close is idempotent.
func (e *Exchanger) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	for k, w := range e.waiting {
		w.ch <- result{err: ErrClosed}
		delete(e.waiting, k)
	}
	e.mu.Unlock()
}
