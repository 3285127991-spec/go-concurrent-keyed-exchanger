// Command exchanger 演示 exchanger 库的基本用法：
// 一次正常配对、一次 context 取消，以及 Close 的行为。
package main

import (
	"context"
	"fmt"
	"runtime"

	"keyedexchanger/exchanger"
)

type outcome struct {
	value any
	err   error
}

func call(e *exchanger.Exchanger, ctx context.Context, key string, value any) <-chan outcome {
	ch := make(chan outcome, 1)
	go func() {
		v, err := e.Exchange(ctx, key, value)
		ch <- outcome{value: v, err: err}
	}()
	return ch
}

// waitWaiting 有界自旋等待等待者数量达到 n，仅用于让演示输出有序。
func waitWaiting(e *exchanger.Exchanger, n int) {
	for i := 0; i < 10_000_000 && e.Len() != n; i++ {
		runtime.Gosched()
	}
}

func main() {
	e := exchanger.New()

	// 1. 正常配对：双方各自拿到对方传入的值。
	first := call(e, context.Background(), "greeting", "hello")
	waitWaiting(e, 1)
	got, err := e.Exchange(context.Background(), "greeting", "world")
	fmt.Printf("[pair]   second Exchange got %q (err=%v)\n", got, err)
	out := <-first
	fmt.Printf("[pair]   first Exchange got %q (err=%v)\n", out.value, out.err)

	// 2. context 取消：只移除自己，返回 ctx.Err()。
	ctx, cancel := context.WithCancel(context.Background())
	waiting := call(e, ctx, "cancel-demo", "v1")
	waitWaiting(e, 1)
	cancel()
	out = <-waiting
	fmt.Printf("[cancel] cancelled Exchange err=%v, Len=%d\n", out.err, e.Len())

	// 3. Close：唤醒未配对的等待者，并拒绝新的 Exchange。
	pending := call(e, context.Background(), "close-demo", "v2")
	waitWaiting(e, 1)
	e.Close()
	out = <-pending
	fmt.Printf("[close]  pending Exchange err=%v\n", out.err)
	_, err = e.Exchange(context.Background(), "close-demo", "v3")
	fmt.Printf("[close]  Exchange after Close err=%v\n", err)
}
