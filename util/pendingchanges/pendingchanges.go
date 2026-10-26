// Package pendingchanges keeps the changes a daemon reported and the worker has
// not applied yet: the names of the objects and instances whose status changed,
// one space separated list per node in a redis hash field.
//
// The worker only writes in full the objects and instances listed, the others
// having their timestamps renewed: a change lost leaves their status stale
// until the next one. The feeder merging and the worker taking the list run
// concurrently, so both are atomic.
package pendingchanges

import (
	"context"
	"errors"
	"strings"

	"github.com/go-redis/redis/v8"
)

// mergeScript adds the words of ARGV[2] to those of the field ARGV[1] of the
// hash KEYS[1], each once, in a single step no other client interleaves with.
var mergeScript = redis.NewScript(`
local seen, out = {}, {}
local function add(s)
  for w in string.gmatch(s, "%S+") do
    if not seen[w] then
      seen[w] = true
      out[#out + 1] = w
    end
  end
end
add(redis.call("HGET", KEYS[1], ARGV[1]) or "")
add(ARGV[2])
redis.call("HSET", KEYS[1], ARGV[1], table.concat(out, " "))
return #out
`)

// Merge adds changes to those pending in the field of the hash key.
func Merge(ctx context.Context, c redis.Cmdable, key, field string, changes []string) error {
	if len(changes) == 0 {
		return nil
	}
	return mergeScript.Run(ctx, c, []string{key}, field, strings.Join(changes, " ")).Err()
}

// Take returns the changes pending in the field of the hash key and removes
// them, in one transaction: a change merged meanwhile is either returned or
// left for the next Take.
func Take(ctx context.Context, c redis.Cmdable, key, field string) ([]string, error) {
	var get *redis.StringCmd
	_, err := c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		get = p.HGet(ctx, key, field)
		p.HDel(ctx, key, field)
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	s, err := get.Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return strings.Fields(s), nil
}
