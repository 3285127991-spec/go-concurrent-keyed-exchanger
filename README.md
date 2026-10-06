# keyedexchanger

并发安全的 Go keyed exchanger 库：对同一个 key，两个 `Exchange` 调用原子配对并互换 value。仅标准库，无第三方依赖。

## API

- `New() *Exchanger`：创建实例。
- `Exchange(ctx, key, value) (any, error)`：同 key 的第一个调用等待，第二个到达后双方原子配对、各自返回对方的 value；key 为空返回 `ErrEmptyKey`。等待期间 ctx 取消只移除自身并返回 `ctx.Err()`；配对完成后该 key 可进入下一轮独立配对。
- `Snapshot() []string`：按字典序返回仍在等待配对的 key。
- `Len() int`：等待中的 key 数量。
- `Close()`：幂等。唤醒所有未配对调用（返回 `ErrClosed`），之后的 `Exchange` 直接返回 `ErrClosed`；已完成配对的调用不受影响。

## 设计要点

- 单把互斥锁保护 `map[key]*waiter`，每个 waiter 带一个容量为 1 的 channel。
- remove-once 不变量：等待者只会被配对者、取消者、Close 三者之一从 map 摘除，摘除者负责向 channel 投递恰好一个结果，因此发送永不阻塞、不会重复配对。
- 取消与配对竞争时，持锁顺序决定唯一结果：取消先生效则后到者成为新的等待者；配对先生效则双方正常返回。

## 示例

```go
e := exchanger.New()
defer e.Close()

go func() {
    v, _ := e.Exchange(context.Background(), "greeting", "hello")
    fmt.Println(v) // "world"
}()

v, _ := e.Exchange(context.Background(), "greeting", "world")
fmt.Println(v) // "hello"
```

## CLI 演示

```sh
go run ./cmd/exchanger
```

依次演示一次正常配对、一次 context 取消、Close 唤醒等待者并拒绝新调用。

## 测试

```sh
go test -race ./...
```

