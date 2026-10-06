# keyedexchanger

A concurrency-safe keyed exchanger for Go. Callers pair up by string key: the
first `Exchange` for a key blocks until a second call with the same key
arrives, then both return each other's value. Pure standard library, no
third-party dependencies.

## Semantics

- The first `Exchange(ctx, key, v)` waits; the second pairs with it
  atomically. After pairing, the key is free for a new, independent round.
- Different keys are fully independent and never share results.
- Cancelling a waiting context removes only that call. If cancellation races
  with an arriving partner, exactly one outcome stands: the cancel wins and
  the partner becomes the new waiter, or the pair completes and both return.
- `Close` wakes all waiting callers with `ErrClosed` and rejects new
  exchanges; already-paired calls are unaffected. `Close` is idempotent.
- Empty keys are rejected with `ErrEmptyKey`.

## API

```go
e := keyedexchanger.New()
defer e.Close()

got, err := e.Exchange(ctx, "key", myValue) // blocks until a partner arrives
waiting := e.Snapshot()                     // sorted keys still waiting
n := e.Len()                                // number of waiting keys
e.Close()                                   // wake waiters, reject new calls
```

Errors: `ErrEmptyKey`, `ErrClosed`, and the waiting context's `ctx.Err()`.

## Layout

- `exchanger.go` — the library (package `keyedexchanger`)
- `exchanger_test.go` — tests: pairing, rounds, keys, cancel, cancel/pair
  race, snapshot/len, empty key, close, concurrency
- `cmd/exchanger-demo` — minimal CLI demo (one pair, one cancel, one close)

## Run

```sh
go test -race ./...
go run ./cmd/exchanger-demo
```
