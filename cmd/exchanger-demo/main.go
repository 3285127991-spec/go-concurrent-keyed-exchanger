// Command exchanger-demo demonstrates one successful pairing, one
// cancellation, and Close, using the keyedexchanger library.
package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"keyedexchanger"
)

func waitForLen(e *keyedexchanger.Exchanger, n int) {
	for e.Len() != n {
		runtime.Gosched()
	}
}

func main() {
	demoPair()
	demoCancel()
	demoClose()
}

func demoPair() {
	fmt.Println("== pair ==")
	e := keyedexchanger.New()
	defer e.Close()

	first := make(chan any, 1)
	go func() {
		v, err := e.Exchange(context.Background(), "greeting", "hello")
		if err != nil {
			first <- err
			return
		}
		first <- v
	}()
	waitForLen(e, 1)

	got, err := e.Exchange(context.Background(), "greeting", "world")
	fmt.Printf("second caller sent %q, received %q (err=%v)\n", "world", got, err)
	fmt.Printf("first caller sent %q, received %q\n", "hello", <-first)
}

func demoCancel() {
	fmt.Println("== cancel ==")
	e := keyedexchanger.New()
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := e.Exchange(ctx, "job", "payload")
		done <- err
	}()
	waitForLen(e, 1)
	fmt.Printf("waiting keys before cancel: %v\n", e.Snapshot())

	cancel()
	fmt.Printf("cancelled caller err: %v\n", <-done)
	fmt.Printf("waiting keys after cancel: %v (len=%d)\n", e.Snapshot(), e.Len())
}

func demoClose() {
	fmt.Println("== close ==")
	e := keyedexchanger.New()

	done := make(chan error, 1)
	go func() {
		_, err := e.Exchange(context.Background(), "pending", "data")
		done <- err
	}()
	waitForLen(e, 1)

	e.Close()
	err := <-done
	fmt.Printf("waiting caller woke with: %v (ErrClosed=%v)\n", err, errors.Is(err, keyedexchanger.ErrClosed))

	_, err = e.Exchange(context.Background(), "late", "x")
	fmt.Printf("exchange after close: %v (ErrClosed=%v)\n", err, errors.Is(err, keyedexchanger.ErrClosed))
}
