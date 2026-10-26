package pendingchanges

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

const key = "h"

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestMergeThenTake(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	if l, err := Take(ctx, c, key, "n1"); err != nil || l != nil {
		t.Fatalf("take on nothing pending: %v, %v", l, err)
	}
	if err := Merge(ctx, c, key, "n1", []string{"a", "a@n1"}); err != nil {
		t.Fatal(err)
	}
	if err := Merge(ctx, c, key, "n1", []string{"a@n1", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := Merge(ctx, c, key, "n2", []string{"c"}); err != nil {
		t.Fatal(err)
	}
	if err := Merge(ctx, c, key, "n1", nil); err != nil {
		t.Fatal(err)
	}
	l, err := Take(ctx, c, key, "n1")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(l)
	if want := []string{"a", "a@n1", "b"}; !slices.Equal(l, want) {
		t.Errorf("took %v, want %v", l, want)
	}
	if l, err := Take(ctx, c, key, "n1"); err != nil || l != nil {
		t.Errorf("taken twice: %v, %v", l, err)
	}
	if l, err := Take(ctx, c, key, "n2"); err != nil || !slices.Equal(l, []string{"c"}) {
		t.Errorf("the other field: %v, %v", l, err)
	}
}

// TestConcurrentMergeAndTakeLoseNothing merges changes while others take them:
// every change merged is taken once.
func TestConcurrentMergeAndTakeLoseNothing(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	const writers, perWriter = 8, 50
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		taken = make(map[string]int)
		done  = make(chan struct{})
	)
	take := func() {
		l, err := Take(ctx, c, key, "n1")
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		for _, s := range l {
			taken[s]++
		}
		mu.Unlock()
	}
	var readers sync.WaitGroup
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
					take()
				}
			}
		}()
	}
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				if err := Merge(ctx, c, key, "n1", []string{fmt.Sprintf("o%d-%d", w, i)}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	close(done)
	readers.Wait()
	take()
	if len(taken) != writers*perWriter {
		t.Errorf("took %d changes, want %d", len(taken), writers*perWriter)
	}
	for s, n := range taken {
		if n != 1 {
			t.Errorf("%s taken %d times", s, n)
		}
	}
}
