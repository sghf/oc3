package worker

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
)

func TestInstanceActionDropData(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = c.Close() })

	d := newAction("svc1", "n1", "c1", "u1")
	d.SetRedis(c)
	key := cachekeys.FeedInstanceActionH

	// The payload read is removed once processed.
	if err := c.HSet(ctx, key, d.idX, `{"begin":"b"}`).Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.getData(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.dropData(ctx); err != nil {
		t.Fatal(err)
	}
	if n := c.HLen(ctx, key).Val(); n != 0 {
		t.Fatalf("%d payloads left, want 0", n)
	}

	// The end of the action, posted after its begin was read, is kept for
	// the job its post queued.
	if err := c.HSet(ctx, key, d.idX, `{"begin":"b"}`).Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.getData(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, key, d.idX, `{"begin":"b","end":"e"}`).Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.dropData(ctx); err != nil {
		t.Fatal(err)
	}
	if v := c.HGet(ctx, key, d.idX).Val(); v != `{"begin":"b","end":"e"}` {
		t.Fatalf("payload %q, want the end kept", v)
	}

	// A payload that does not decode is removed as well.
	if err := c.HSet(ctx, key, d.idX, `not json`).Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.getData(ctx); err == nil {
		t.Fatal("getData decoded an invalid payload")
	}
	if err := d.dropData(ctx); err != nil {
		t.Fatal(err)
	}
	if n := c.HLen(ctx, key).Val(); n != 0 {
		t.Fatalf("%d payloads left, want 0", n)
	}
}
