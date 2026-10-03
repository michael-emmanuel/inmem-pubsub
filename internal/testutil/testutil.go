// Package testutil holds small helpers for testing concurrent code. It is
// internal so that it does not become part of the public API.
package testutil

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// Eventually polls cond every few milliseconds until it returns true or the
// timeout elapses, then fails the test. Use it to wait for asynchronous state
// (for example context-driven cancellation) instead of sleeping for a fixed
// time.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v: %s", timeout, msg)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// WaitTimeout waits for wg and fails the test (rather than hanging until the
// global test timeout) if it does not finish in time. A hang here usually
// means a deadlock in the code under test.
func WaitTimeout(t testing.TB, wg *sync.WaitGroup, timeout time.Duration, what string) {
	t.Helper()
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ch:
	case <-time.After(timeout):
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("timed out after %v waiting for %s (possible deadlock)\n%s", timeout, what, buf[:n])
	}
}

// LeakCheck records the current goroutine count and registers a cleanup that
// fails the test if the count has not returned to that level shortly after the
// test body (and cleanups registered later) finish. Tests using it must not
// call t.Parallel, because other tests' goroutines would be miscounted.
func LeakCheck(t testing.TB) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if runtime.NumGoroutine() <= before {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<16)
				n := runtime.Stack(buf, true)
				t.Errorf("goroutine leak: %d before, %d after\n%s", before, runtime.NumGoroutine(), buf[:n])
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
}
