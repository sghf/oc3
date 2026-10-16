package serverhandlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"golang.org/x/sync/singleflight"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// pkgDiffCacheTTL bounds how long a packages diff is served from the cache,
// whatever the generations say: a change no feed announces (a fix made in the
// database) shows within that time.
const pkgDiffCacheTTL = time.Hour

// errPkgDiffBusy is returned to a caller who waited longer than the packages diff
// takes room: the computation goes on, its result cached for the next request.
var errPkgDiffBusy = errors.New("the packages diff is being computed")

// pkgDiffRunner computes the packages diffs: one computation for the requests
// asking the same diff at the same time, at most `slots` computations at once, each
// detached from the request that started it, so that it ends and fills the cache
// even when its callers gave up.
type pkgDiffRunner struct {
	flight  singleflight.Group
	slots   chan struct{}
	timeout time.Duration
}

func newPkgDiffRunner(slots int, timeout time.Duration) *pkgDiffRunner {
	if slots < 1 {
		slots = 1
	}
	return &pkgDiffRunner{slots: make(chan struct{}, slots), timeout: timeout}
}

// run computes the diff of `key`, or joins the computation under way, and waits
// for it as long as ctx lets it.
func (r *pkgDiffRunner) run(ctx context.Context, key string, compute func(context.Context) (any, error)) (any, error) {
	ch := r.flight.DoChan(key, func() (any, error) {
		cctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		select {
		case r.slots <- struct{}{}:
		case <-cctx.Done():
			return nil, errPkgDiffBusy
		}
		defer func() { <-r.slots }()
		return compute(cctx)
	})
	select {
	case res := <-ch:
		return res.Val, res.Err
	case <-ctx.Done():
		return nil, errPkgDiffBusy
	}
}

var (
	pkgDiffRunnerOnce sync.Once
	pkgDiffShared     *pkgDiffRunner
)

// pkgDiffTimeout is how long a packages diff may take, and a request wait for it.
func pkgDiffTimeout() time.Duration {
	if d := viper.GetDuration("server.packages_diff.timeout"); d > 0 {
		return d
	}
	return 30 * time.Second
}

// sharedPkgDiffRunner is the runner of the server, sized by its configuration.
func sharedPkgDiffRunner() *pkgDiffRunner {
	pkgDiffRunnerOnce.Do(func() {
		pkgDiffShared = newPkgDiffRunner(viper.GetInt("server.packages_diff.concurrency"), pkgDiffTimeout())
	})
	return pkgDiffShared
}

// pkgDiffCacheKey is the cache key of the diff of the nodes, whatever their
// order, at the generations of their packages: a node pushing new packages
// renews its generation, and so the key of every diff comparing it.
func pkgDiffCacheKey(nodeIDs, nodeGens []string, globalGen string) string {
	pairs := make([]string, len(nodeIDs))
	for i, id := range nodeIDs {
		gen := ""
		if i < len(nodeGens) {
			gen = nodeGens[i]
		}
		pairs[i] = id + "=" + gen
	}
	sort.Strings(pairs)
	sum := sha256.Sum256([]byte(globalGen + "|" + strings.Join(pairs, ",")))
	return cachekeys.PkgDiffPrefix + hex.EncodeToString(sum[:])
}

// pkgDiffGenerations reads the generations of the packages of the nodes and of
// all of them, "" for one never renewed.
func pkgDiffGenerations(ctx context.Context, rdb *redis.Client, nodeIDs []string) ([]string, string, error) {
	keys := make([]string, 0, len(nodeIDs)+1)
	for _, id := range nodeIDs {
		keys = append(keys, cachekeys.PkgDiffGenPrefix+id)
	}
	keys = append(keys, cachekeys.PkgDiffGen)
	vals, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, "", err
	}
	gens := make([]string, len(vals))
	for i, v := range vals {
		if s, ok := v.(string); ok {
			gens[i] = s
		}
	}
	return gens[:len(nodeIDs)], gens[len(nodeIDs)], nil
}

// packagesDiff returns the diff of the nodes, from the cache when the packages of
// none of them changed since it was computed, else computed by the runner of the
// server and cached.
func (a *Api) packagesDiff(ctx context.Context, log *slog.Logger, nodeIDs []string) ([]cdb.PackageDiffRow, error) {
	return cachedPkgDiff(ctx, log, a.Redis, sharedPkgDiffRunner(), nodeIDs, a.ODB.PackagesDiff)
}

// cachedPkgDiff returns the diff compute gives for the nodes, from the cache of
// rdb when one is fresh. Without Redis, or when it fails, the diff is computed
// every time, still under the runner's limits.
func cachedPkgDiff(ctx context.Context, log *slog.Logger, rdb *redis.Client, runner *pkgDiffRunner, nodeIDs []string,
	compute func(context.Context, []string) ([]cdb.PackageDiffRow, error)) ([]cdb.PackageDiffRow, error) {
	sorted := append([]string{}, nodeIDs...)
	sort.Strings(sorted)
	key := ""
	if rdb != nil {
		if gens, global, err := pkgDiffGenerations(ctx, rdb, sorted); err != nil {
			log.Warn("cannot read the packages diff generations", logkey.Error, err)
		} else {
			key = pkgDiffCacheKey(sorted, gens, global)
			if raw, err := rdb.Get(ctx, key).Bytes(); err == nil {
				var rows []cdb.PackageDiffRow
				if err := json.Unmarshal(raw, &rows); err == nil {
					return rows, nil
				}
			} else if !errors.Is(err, redis.Nil) {
				log.Warn("cannot read the packages diff cache", logkey.Error, err)
			}
		}
	}
	flightKey := key
	if flightKey == "" {
		flightKey = strings.Join(sorted, ",")
	}
	val, err := runner.run(ctx, flightKey, func(cctx context.Context) (any, error) {
		rows, err := compute(cctx, sorted)
		if err != nil {
			return nil, err
		}
		if key != "" {
			if b, err := json.Marshal(rows); err == nil {
				if err := rdb.Set(cctx, key, b, pkgDiffCacheTTL).Err(); err != nil {
					log.Warn("cannot cache the packages diff", logkey.Error, err)
				}
			}
		}
		return rows, nil
	})
	if err != nil {
		return nil, err
	}
	rows, _ := val.([]cdb.PackageDiffRow)
	return rows, nil
}
