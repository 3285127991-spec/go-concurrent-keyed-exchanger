package exchanger

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

type exchangeOutcome struct {
	value any
	err   error
}

// callExchange 在 goroutine 中执行一次 Exchange，结果写入缓冲 channel。
func callExchange(e *Exchanger, ctx context.Context, key string, value any) <-chan exchangeOutcome {
	ch := make(chan exchangeOutcome, 1)
	go func() {
		v, err := e.Exchange(ctx, key, value)
		ch <- exchangeOutcome{value: v, err: err}
	}()
	return ch
}

// recv 接收一次 Exchange 结果；超时仅作为防止测试永久阻塞的失败保护。
func recv(t *testing.T, ch <-chan exchangeOutcome) exchangeOutcome {
	t.Helper()
	select {
	case out := <-ch:
		return out
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Exchange to return")
		return exchangeOutcome{}
	}
}

// waitForLen 有界自旋等待等待者数量达到 want，避免用 sleep 做同步。
func waitForLen(t *testing.T, e *Exchanger, want int) {
	t.Helper()
	for i := 0; i < 5_000_000; i++ {
		if e.Len() == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("Len() did not reach %d", want)
}

func TestExchangePairsTwoCalls(t *testing.T) {
	e := New()
	defer e.Close()

	first := callExchange(e, context.Background(), "greeting", "hello")
	waitForLen(t, e, 1)

	got, err := e.Exchange(context.Background(), "greeting", "world")
	if err != nil {
		t.Fatalf("second Exchange: %v", err)
	}
	if got != "hello" {
		t.Fatalf("second Exchange got %v, want %q", got, "hello")
	}
	out := recv(t, first)
	if out.err != nil || out.value != "world" {
		t.Fatalf("first Exchange got (%v, %v), want (%q, nil)", out.value, out.err, "world")
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len() = %d, want 0 after pairing", n)
	}
	if s := e.Snapshot(); len(s) != 0 {
		t.Fatalf("Snapshot() = %v, want empty after pairing", s)
	}
}

func TestExchangeSameKeyMultipleRounds(t *testing.T) {
	e := New()
	defer e.Close()

	for round := 0; round < 5; round++ {
		a := fmt.Sprintf("a-%d", round)
		b := fmt.Sprintf("b-%d", round)
		first := callExchange(e, context.Background(), "key", a)
		waitForLen(t, e, 1)

		got, err := e.Exchange(context.Background(), "key", b)
		if err != nil || got != a {
			t.Fatalf("round %d: second got (%v, %v), want (%q, nil)", round, got, err, a)
		}
		out := recv(t, first)
		if out.err != nil || out.value != b {
			t.Fatalf("round %d: waiter got (%v, %v), want (%q, nil)", round, out.value, out.err, b)
		}
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len() = %d, want 0 after all rounds", n)
	}
}

func TestExchangeDifferentKeysIndependent(t *testing.T) {
	e := New()
	defer e.Close()

	chA := callExchange(e, context.Background(), "a", "A")
	chB := callExchange(e, context.Background(), "b", "B")
	waitForLen(t, e, 2)

	// 两个 key 的等待者必须各自独立等待，不能互相配对。
	if got := e.Snapshot(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("Snapshot() = %v, want [a b]", got)
	}

	gotB, err := e.Exchange(context.Background(), "b", "B2")
	if err != nil || gotB != "B" {
		t.Fatalf("pair b: got (%v, %v), want (%q, nil)", gotB, err, "B")
	}
	outB := recv(t, chB)
	if outB.err != nil || outB.value != "B2" {
		t.Fatalf("waiter b got (%v, %v), want (%q, nil)", outB.value, outB.err, "B2")
	}

	// a 的等待者不受 b 配对的影响。
	waitForLen(t, e, 1)
	gotA, err := e.Exchange(context.Background(), "a", "A2")
	if err != nil || gotA != "A" {
		t.Fatalf("pair a: got (%v, %v), want (%q, nil)", gotA, err, "A")
	}
	outA := recv(t, chA)
	if outA.err != nil || outA.value != "A2" {
		t.Fatalf("waiter a got (%v, %v), want (%q, nil)", outA.value, outA.err, "A2")
	}
}

func TestExchangeEmptyKey(t *testing.T) {
	e := New()
	defer e.Close()

	if _, err := e.Exchange(context.Background(), "", "x"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: got err %v, want ErrEmptyKey", err)
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len() = %d, want 0 (empty key must not register a waiter)", n)
	}
}

func TestExchangeContextCancel(t *testing.T) {
	e := New()
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())
	first := callExchange(e, ctx, "key", "v1")
	waitForLen(t, e, 1)

	cancel()
	out := recv(t, first)
	if !errors.Is(out.err, context.Canceled) {
		t.Fatalf("cancelled Exchange: got err %v, want context.Canceled", out.err)
	}
	waitForLen(t, e, 0)

	// 取消后该 key 可以正常进入新一轮配对。
	next := callExchange(e, context.Background(), "key", "v2")
	waitForLen(t, e, 1)
	got, err := e.Exchange(context.Background(), "key", "v3")
	if err != nil || got != "v2" {
		t.Fatalf("next round: got (%v, %v), want (%q, nil)", got, err, "v2")
	}
	outNext := recv(t, next)
	if outNext.err != nil || outNext.value != "v3" {
		t.Fatalf("next round waiter: got (%v, %v), want (%q, nil)", outNext.value, outNext.err, "v3")
	}
}

func TestExchangeCancelRemovesOnlyItself(t *testing.T) {
	e := New()
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())
	chA := callExchange(e, ctx, "a", "A")
	chB := callExchange(e, context.Background(), "b", "B")
	waitForLen(t, e, 2)

	cancel()
	outA := recv(t, chA)
	if !errors.Is(outA.err, context.Canceled) {
		t.Fatalf("cancelled a: got err %v, want context.Canceled", outA.err)
	}
	waitForLen(t, e, 1)
	if got := e.Snapshot(); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("Snapshot() = %v, want [b]", got)
	}

	got, err := e.Exchange(context.Background(), "b", "B2")
	if err != nil || got != "B" {
		t.Fatalf("pair b: got (%v, %v), want (%q, nil)", got, err, "B")
	}
	outB := recv(t, chB)
	if outB.err != nil || outB.value != "B2" {
		t.Fatalf("waiter b got (%v, %v), want (%q, nil)", outB.value, outB.err, "B2")
	}
}

func TestExchangeCancelPairRace(t *testing.T) {
	for i := 0; i < 2000; i++ {
		e := New()
		ctx, cancel := context.WithCancel(context.Background())
		first := callExchange(e, ctx, "key", "first")
		waitForLen(t, e, 1)

		second := callExchange(e, context.Background(), "key", "second")
		cancel()

		out1 := recv(t, first)
		if errors.Is(out1.err, context.Canceled) {
			// 取消先生效：second 成为新的等待者，由第三个调用完成配对。
			got, err := e.Exchange(context.Background(), "key", "third")
			if err != nil || got != "second" {
				t.Fatalf("iter %d: third Exchange got (%v, %v), want (%q, nil)", i, got, err, "second")
			}
			out2 := recv(t, second)
			if out2.err != nil || out2.value != "third" {
				t.Fatalf("iter %d: second got (%v, %v), want (%q, nil)", i, out2.value, out2.err, "third")
			}
		} else {
			// 配对先生效：双方各自拿到对方的值。
			if out1.err != nil || out1.value != "second" {
				t.Fatalf("iter %d: first got (%v, %v), want (%q, nil)", i, out1.value, out1.err, "second")
			}
			out2 := recv(t, second)
			if out2.err != nil || out2.value != "first" {
				t.Fatalf("iter %d: second got (%v, %v), want (%q, nil)", i, out2.value, out2.err, "first")
			}
		}
		if n := e.Len(); n != 0 {
			t.Fatalf("iter %d: Len() = %d, want 0", i, n)
		}
		e.Close()
	}
}

func TestSnapshotAndLen(t *testing.T) {
	e := New()
	defer e.Close()

	if got := e.Snapshot(); len(got) != 0 {
		t.Fatalf("initial Snapshot() = %v, want empty", got)
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("initial Len() = %d, want 0", n)
	}

	callExchange(e, context.Background(), "b", "1")
	callExchange(e, context.Background(), "a", "2")
	callExchange(e, context.Background(), "c", "3")
	waitForLen(t, e, 3)

	if got := e.Snapshot(); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("Snapshot() = %v, want [a b c]", got)
	}

	if _, err := e.Exchange(context.Background(), "b", "x"); err != nil {
		t.Fatalf("pair b: %v", err)
	}
	waitForLen(t, e, 2)
	if got := e.Snapshot(); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("Snapshot() = %v, want [a c]", got)
	}
	if n := e.Len(); n != 2 {
		t.Fatalf("Len() = %d, want 2", n)
	}
}

func TestClose(t *testing.T) {
	e := New()

	// 先完成一对配对，已完成的配对不受 Close 影响。
	done := callExchange(e, context.Background(), "done", "d1")
	waitForLen(t, e, 1)
	if got, err := e.Exchange(context.Background(), "done", "d2"); err != nil || got != "d1" {
		t.Fatalf("pair before close: got (%v, %v), want (%q, nil)", got, err, "d1")
	}
	outDone := recv(t, done)
	if outDone.err != nil || outDone.value != "d2" {
		t.Fatalf("waiter before close: got (%v, %v), want (%q, nil)", outDone.value, outDone.err, "d2")
	}

	pending1 := callExchange(e, context.Background(), "p1", "v1")
	pending2 := callExchange(e, context.Background(), "p2", "v2")
	waitForLen(t, e, 2)

	e.Close()

	for _, ch := range []<-chan exchangeOutcome{pending1, pending2} {
		out := recv(t, ch)
		if !errors.Is(out.err, ErrClosed) {
			t.Fatalf("pending Exchange after Close: got err %v, want ErrClosed", out.err)
		}
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len() after Close = %d, want 0", n)
	}
	if got := e.Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot() after Close = %v, want empty", got)
	}
	if _, err := e.Exchange(context.Background(), "p1", "v3"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Exchange after Close: got err %v, want ErrClosed", err)
	}

	// Close 幂等，重复调用不应 panic。
	e.Close()
}

type record struct {
	key      string
	sent     any
	received any
}

func TestExchangeConcurrentPairs(t *testing.T) {
	e := New()
	defer e.Close()

	const keyCount = 8
	const pairsPerKey = 25
	const wantRecords = keyCount * pairsPerKey * 2

	results := make(chan record, wantRecords)
	var wg sync.WaitGroup
	for k := 0; k < keyCount; k++ {
		key := fmt.Sprintf("key-%d", k)
		for p := 0; p < pairsPerKey*2; p++ {
			value := fmt.Sprintf("%s-value-%d", key, p)
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := e.Exchange(context.Background(), key, value)
				if err != nil {
					t.Errorf("Exchange(%s): %v", key, err)
					return
				}
				results <- record{key: key, sent: value, received: got}
			}()
		}
	}
	wg.Wait()
	close(results)

	sent := make(map[string]map[any]int)
	received := make(map[string]map[any]int)
	count := 0
	for r := range results {
		count++
		if r.sent == r.received {
			t.Errorf("key %s: participant received its own value %v", r.key, r.sent)
		}
		if sent[r.key] == nil {
			sent[r.key] = make(map[any]int)
			received[r.key] = make(map[any]int)
		}
		sent[r.key][r.sent]++
		received[r.key][r.received]++
	}
	if count != wantRecords {
		t.Fatalf("got %d results, want %d", count, wantRecords)
	}
	for key, want := range sent {
		if got := received[key]; !reflect.DeepEqual(got, want) {
			t.Errorf("key %s: received multiset %v != sent multiset %v", key, got, want)
		}
	}
	if n := e.Len(); n != 0 {
		t.Fatalf("Len() = %d, want 0 after all pairs", n)
	}
}
