package serverhandlers

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

func TestPkgDiffCacheKey(t *testing.T) {
	a := pkgDiffCacheKey([]string{"n1", "n2"}, []string{"3", ""}, "1")
	if b := pkgDiffCacheKey([]string{"n2", "n1"}, []string{"", "3"}, "1"); a != b {
		t.Errorf("the order of the nodes changes the key: %s, %s", a, b)
	}
	for name, other := range map[string]string{
		"a node generation": pkgDiffCacheKey([]string{"n1", "n2"}, []string{"4", ""}, "1"),
		"the global one":    pkgDiffCacheKey([]string{"n1", "n2"}, []string{"3", ""}, "2"),
		"another node":      pkgDiffCacheKey([]string{"n1", "n3"}, []string{"3", ""}, "1"),
	} {
		if other == a {
			t.Errorf("%s does not change the key", name)
		}
	}
}

func TestPkgDiffRunnerJoinsAndLimits(t *testing.T) {
	r := newPkgDiffRunner(1, time.Second)
	var calls, running, most atomic.Int32
	release := make(chan struct{})
	compute := func(context.Context) (any, error) {
		calls.Add(1)
		if n := running.Add(1); n > most.Load() {
			most.Store(n)
		}
		<-release
		running.Add(-1)
		return "rows", nil
	}
	var wg sync.WaitGroup
	results := make(chan any, 6)
	// Three requests of one diff, three of another: one computation each, never
	// two at once with a single slot.
	for _, key := range []string{"a", "a", "a", "b", "b", "b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			v, err := r.run(context.Background(), key, compute)
			if err != nil {
				t.Error(err)
			}
			results <- v
		}(key)
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 2 || most.Load() != 1 {
		t.Errorf("calls %d, most at once %d; want 2 and 1", calls.Load(), most.Load())
	}
	if len(results) != 6 {
		t.Errorf("%d results, want 6", len(results))
	}
}

func TestPkgDiffRunnerCallerLeaves(t *testing.T) {
	r := newPkgDiffRunner(2, time.Second)
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := r.run(ctx, "slow", func(cctx context.Context) (any, error) {
		defer close(done)
		select {
		case <-time.After(200 * time.Millisecond):
			return "rows", nil
		case <-cctx.Done():
			return nil, cctx.Err()
		}
	})
	if !errors.Is(err, errPkgDiffBusy) {
		t.Errorf("err = %v, want errPkgDiffBusy", err)
	}
	// The computation goes on, detached from the caller.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the computation stopped with its caller")
	}
}

func TestCachedPkgDiff(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	log := slog.New(slog.DiscardHandler)
	r := newPkgDiffRunner(2, time.Second)
	var calls int
	compute := func(_ context.Context, ids []string) ([]cdb.PackageDiffRow, error) {
		calls++
		if len(ids) != 2 || ids[0] != "n1" {
			t.Errorf("ids = %q, want them sorted", ids)
		}
		return []cdb.PackageDiffRow{{NodeID: "n1", PkgName: "bash", PkgVersion: "5.2"}}, nil
	}
	ctx := context.Background()
	get := func() []cdb.PackageDiffRow {
		t.Helper()
		rows, err := cachedPkgDiff(ctx, log, rdb, r, []string{"n2", "n1"}, compute)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	if rows := get(); len(rows) != 1 || rows[0].PkgName != "bash" || calls != 1 {
		t.Fatalf("first: rows %+v, calls %d", rows, calls)
	}
	if rows := get(); len(rows) != 1 || calls != 1 {
		t.Errorf("second, from the cache: rows %+v, calls %d", rows, calls)
	}
	// A node pushes new packages: computed again, then cached again.
	mr.Incr(cachekeys.PkgDiffGenPrefix+"n2", 1)
	get()
	get()
	if calls != 2 {
		t.Errorf("after a node generation, calls %d, want 2", calls)
	}
	// The packages are scrubbed: every diff computed again.
	mr.Incr(cachekeys.PkgDiffGen, 1)
	get()
	if calls != 3 {
		t.Errorf("after the global generation, calls %d, want 3", calls)
	}
	// Without redis: computed every time.
	if _, err := cachedPkgDiff(ctx, log, nil, r, []string{"n1", "n2"}, compute); err != nil || calls != 4 {
		t.Errorf("without redis: calls %d, err %v", calls, err)
	}
}
