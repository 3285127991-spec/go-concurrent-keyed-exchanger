package keyedexchanger

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

type exchangeResult struct {
	value any
	err   error
}

// waitForLen spins until the exchanger reports want waiting keys. It uses no
// sleeps; the timer is only a failsafe so a broken implementation fails the
// test instead of hanging forever.
func waitForLen(t *testing.T, e *Exchanger, want int) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for e.Len() != want {
		select {
		case <-timer.C:
			t.Fatalf("timed out waiting for Len()=%d, got %d", want, e.Len())
		default:
			runtime.Gosched()
		}
	}
}

func exchangeAsync(e *Exchanger, ctx context.Context, key string, value any) <-chan exchangeResult {
	ch := make(chan exchangeResult, 1)
	go func() {
		v, err := e.Exchange(ctx, key, value)
		ch <- exchangeResult{value: v, err: err}
	}()
	return ch
}

func TestBasicExchange(t *testing.T) {
	e := New()
	defer e.Close()

	first := exchangeAsync(e, context.Background(), "greeting", "hello")
	waitForLen(t, e, 1)

	got, err := e.Exchange(context.Background(), "greeting", "world")
	if err != nil {
		t.Fatalf("second Exchange: %v", err)
	}
	if got != "hello" {
		t.Fatalf("second Exchange got %v, want %q", got, "hello")
	}
	r := <-first
	if r.err != nil {
		t.Fatalf("first Exchange: %v", r.err)
	}
	if r.value != "world" {
		t.Fatalf("first Exchange got %v, want %q", r.value, "world")
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len()=%d after pairing, want 0", n)
	}
}

func TestSameKeyMultipleRounds(t *testing.T) {
	e := New()
	defer e.Close()

	for round := 0; round < 3; round++ {
		a := fmt.Sprintf("a-%d", round)
		b := fmt.Sprintf("b-%d", round)
		first := exchangeAsync(e, context.Background(), "key", a)
		waitForLen(t, e, 1)

		got, err := e.Exchange(context.Background(), "key", b)
		if err != nil {
			t.Fatalf("round %d second Exchange: %v", round, err)
		}
		if got != a {
			t.Fatalf("round %d: second got %v, want %q", round, got, a)
		}
		r := <-first
		if r.err != nil || r.value != b {
			t.Fatalf("round %d: first got (%v, %v), want (%q, nil)", round, r.value, r.err, b)
		}
		if n := e.Len(); n != 0 {
			t.Fatalf("round %d: Len()=%d after pairing, want 0", round, n)
		}
	}
}

func TestDifferentKeysIndependent(t *testing.T) {
	e := New()
	defer e.Close()

	waiterA := exchangeAsync(e, context.Background(), "a", "A")
	waiterB := exchangeAsync(e, context.Background(), "b", "B")
	waitForLen(t, e, 2)

	got, err := e.Exchange(context.Background(), "a", "a2")
	if err != nil || got != "A" {
		t.Fatalf("pairing key a: got (%v, %v), want (A, nil)", got, err)
	}
	r := <-waiterA
	if r.err != nil || r.value != "a2" {
		t.Fatalf("waiter on a got (%v, %v), want (a2, nil)", r.value, r.err)
	}
	if n := e.Len(); n != 1 {
		t.Fatalf("Len()=%d after pairing a, want 1 (b still waiting)", n)
	}

	got, err = e.Exchange(context.Background(), "b", "b2")
	if err != nil || got != "B" {
		t.Fatalf("pairing key b: got (%v, %v), want (B, nil)", got, err)
	}
	r = <-waiterB
	if r.err != nil || r.value != "b2" {
		t.Fatalf("waiter on b got (%v, %v), want (b2, nil)", r.value, r.err)
	}
}

func TestEmptyKey(t *testing.T) {
	e := New()
	defer e.Close()

	if _, err := e.Exchange(context.Background(), "", "v"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Exchange with empty key: got %v, want ErrEmptyKey", err)
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len()=%d after rejected call, want 0", n)
	}
}

func TestCancelRemovesOnlySelf(t *testing.T) {
	e := New()
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())
	waiting := exchangeAsync(e, ctx, "k", "v1")
	waitForLen(t, e, 1)
	cancel()

	r := <-waiting
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("cancelled Exchange: got err %v, want context.Canceled", r.err)
	}
	waitForLen(t, e, 0)
	if snap := e.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot()=%v after cancel, want empty", snap)
	}

	// The key must accept a fresh, independent round after the cancel.
	next := exchangeAsync(e, context.Background(), "k", "v2")
	waitForLen(t, e, 1)
	got, err := e.Exchange(context.Background(), "k", "v3")
	if err != nil || got != "v2" {
		t.Fatalf("new round: got (%v, %v), want (v2, nil)", got, err)
	}
	r = <-next
	if r.err != nil || r.value != "v3" {
		t.Fatalf("new round waiter: got (%v, %v), want (v3, nil)", r.value, r.err)
	}
}

func TestCancelPairRace(t *testing.T) {
	for i := 0; i < 32; i++ {
		e := New()
		ctx, cancel := context.WithCancel(context.Background())

		first := exchangeAsync(e, ctx, "k", "first")
		waitForLen(t, e, 1)

		start := make(chan struct{})
		second := make(chan exchangeResult, 1)
		go func() {
			<-start
			v, err := e.Exchange(context.Background(), "k", "second")
			second <- exchangeResult{value: v, err: err}
		}()
		go func() {
			<-start
			cancel()
		}()
		close(start)

		r1 := <-first
		if r1.err != nil {
			// Cancellation won: the second caller must have become the new
			// waiter, so a third call pairs with it.
			if !errors.Is(r1.err, context.Canceled) {
				t.Fatalf("iter %d: first err %v, want context.Canceled", i, r1.err)
			}
			waitForLen(t, e, 1)
			got, err := e.Exchange(context.Background(), "k", "third")
			if err != nil || got != "second" {
				t.Fatalf("iter %d: third got (%v, %v), want (second, nil)", i, got, err)
			}
			r2 := <-second
			if r2.err != nil || r2.value != "third" {
				t.Fatalf("iter %d: second got (%v, %v), want (third, nil)", i, r2.value, r2.err)
			}
		} else {
			// Pairing won: both sides exchanged values.
			if r1.value != "second" {
				t.Fatalf("iter %d: first got %v, want %q", i, r1.value, "second")
			}
			r2 := <-second
			if r2.err != nil || r2.value != "first" {
				t.Fatalf("iter %d: second got (%v, %v), want (first, nil)", i, r2.value, r2.err)
			}
			waitForLen(t, e, 0)
		}
		e.Close()
	}
}

func TestSnapshotAndLen(t *testing.T) {
	e := New()
	defer e.Close()

	if got := e.Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot()=%v on empty exchanger, want empty", got)
	}

	waiters := make([]<-chan exchangeResult, 0, 3)
	for _, key := range []string{"b", "a", "c"} {
		waiters = append(waiters, exchangeAsync(e, context.Background(), key, key))
	}
	waitForLen(t, e, 3)

	snap := e.Snapshot()
	want := []string{"a", "b", "c"}
	if len(snap) != len(want) {
		t.Fatalf("Snapshot()=%v, want %v", snap, want)
	}
	if !sort.StringsAreSorted(snap) {
		t.Fatalf("Snapshot()=%v is not sorted", snap)
	}
	for i := range want {
		if snap[i] != want[i] {
			t.Fatalf("Snapshot()=%v, want %v", snap, want)
		}
	}
	if n := e.Len(); n != 3 {
		t.Fatalf("Len()=%d, want 3", n)
	}

	// Pair everyone off to leave the exchanger clean.
	for _, key := range want {
		if _, err := e.Exchange(context.Background(), key, "x"); err != nil {
			t.Fatalf("pairing key %q: %v", key, err)
		}
	}
	for _, w := range waiters {
		<-w
	}
	waitForLen(t, e, 0)
}

func TestCloseWakesWaiters(t *testing.T) {
	e := New()

	w1 := exchangeAsync(e, context.Background(), "k1", "v1")
	w2 := exchangeAsync(e, context.Background(), "k2", "v2")
	waitForLen(t, e, 2)

	e.Close()

	for i, w := range []<-chan exchangeResult{w1, w2} {
		r := <-w
		if !errors.Is(r.err, ErrClosed) {
			t.Fatalf("waiter %d: got err %v, want ErrClosed", i, r.err)
		}
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len()=%d after Close, want 0", n)
	}
	if snap := e.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot()=%v after Close, want empty", snap)
	}

	// Close is idempotent.
	e.Close()
}

func TestExchangeAfterClose(t *testing.T) {
	e := New()
	e.Close()

	if _, err := e.Exchange(context.Background(), "k", "v"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Exchange after Close: got %v, want ErrClosed", err)
	}
}

func TestConcurrentPairs(t *testing.T) {
	e := New()
	defer e.Close()

	const keys = 4
	const participants = 40 // even per key: 10 callers, 5 pairs

	results := make([]exchangeResult, participants)
	var wg sync.WaitGroup
	for i := 0; i < participants; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%keys)
			v, err := e.Exchange(context.Background(), key, i)
			results[i] = exchangeResult{value: v, err: err}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("participant %d: %v", i, r.err)
		}
		j, ok := r.value.(int)
		if !ok {
			t.Fatalf("participant %d: received non-int %v", i, r.value)
		}
		if j == i {
			t.Fatalf("participant %d paired with itself", i)
		}
		if j%keys != i%keys {
			t.Fatalf("participant %d (key %d) paired with %d (key %d)", i, i%keys, j, j%keys)
		}
		partner, ok := results[j].value.(int)
		if !ok || partner != i {
			t.Fatalf("asymmetric pairing: %d received %d, but %d received %v", i, j, j, results[j].value)
		}
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len()=%d after all pairs, want 0", n)
	}
}
