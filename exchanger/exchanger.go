// Package exchanger 提供并发安全的按 key 配对的交换原语。
//
// 对同一个 key，第一个 Exchange 调用成为等待者并阻塞，第二个到达的调用
// 与它原子配对，双方各自拿到对方传入的 value；配对完成后该 key 可进入
// 下一轮独立配对。不同 key 之间互不影响。
package exchanger

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrEmptyKey 表示传入的 key 为空字符串。
	ErrEmptyKey = errors.New("exchanger: key must be a non-empty string")
	// ErrClosed 表示 Exchanger 已关闭。
	ErrClosed = errors.New("exchanger: closed")
)

// result 是投递给等待者的一次最终结果。
type result struct {
	value any
	err   error
}

// waiter 表示一个正在等待配对的 Exchange 调用。
//
// ch 容量为 1，且整个生命周期内只会被发送恰好一次（remove-once 不变量：
// 等待者只会被配对者、取消者或 Close 之一从 map 中摘除，摘除者负责投递），
// 因此发送永远不会阻塞，也不会重复投递。
type waiter struct {
	value any
	ch    chan result
}

// Exchanger 是并发安全的 keyed exchanger，请使用 New 创建。
type Exchanger struct {
	mu      sync.Mutex
	closed  bool
	waiters map[string]*waiter
}

// New 返回一个可用的 *Exchanger。
func New() *Exchanger {
	return &Exchanger{waiters: make(map[string]*waiter)}
}

// Exchange 用 value 与相同 key 的另一个调用配对，返回对方传入的 value。
//
// 若当前 key 没有等待者，本调用成为等待者并阻塞，直到以下三者之一发生：
//   - 被另一个同 key 的调用配对：返回对方的 value；
//   - ctx 被取消：只移除自身，返回 ctx.Err()；
//   - Exchanger 被 Close：返回 ErrClosed。
//
// 取消与配对并发发生时，只有先生效的一方成立：要么取消成功、后到者成为
// 新的等待者；要么配对已经完成、双方正常返回对方的 value。
func (e *Exchanger) Exchange(ctx context.Context, key string, value any) (any, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	if w, ok := e.waiters[key]; ok {
		// 与当前等待者原子配对：在锁内摘除并投递结果，再返回对方的 value。
		delete(e.waiters, key)
		w.ch <- result{value: value}
		e.mu.Unlock()
		return w.value, nil
	}
	w := &waiter{value: value, ch: make(chan result, 1)}
	e.waiters[key] = w
	e.mu.Unlock()

	select {
	case res := <-w.ch:
		return res.value, res.err
	case <-ctx.Done():
	}

	e.mu.Lock()
	if cur, ok := e.waiters[key]; ok && cur == w {
		// 自己仍在等待：取消生效，只移除自身。
		delete(e.waiters, key)
		e.mu.Unlock()
		return nil, ctx.Err()
	}
	e.mu.Unlock()
	// 取消与配对/Close 并发：自己已被摘除，说明配对或关闭先生效，
	// 结果一定已经缓冲在 w.ch 中，按已完成的结果返回。
	res := <-w.ch
	return res.value, res.err
}

// Snapshot 按字典序返回当前仍在等待配对的 key。
func (e *Exchanger) Snapshot() []string {
	e.mu.Lock()
	keys := make([]string, 0, len(e.waiters))
	for key := range e.waiters {
		keys = append(keys, key)
	}
	e.mu.Unlock()
	sort.Strings(keys)
	return keys
}

// Len 返回当前等待配对的 key 的数量。
func (e *Exchanger) Len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.waiters)
}

// Close 关闭 Exchanger：唤醒所有未配对的等待调用（各自返回 ErrClosed），
// 之后的 Exchange 调用直接返回 ErrClosed。已经完成配对的调用不受影响。
// Close 幂等，可重复调用。
func (e *Exchanger) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	for key, w := range e.waiters {
		delete(e.waiters, key)
		w.ch <- result{err: ErrClosed}
	}
}
