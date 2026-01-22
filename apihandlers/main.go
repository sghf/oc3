package apihandlers

import (
	"database/sql"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	redis "github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cdb"
)

type (
	Api struct {
		DB    *sql.DB
		CDB   *cdb.DB
		Redis *redis.Client
		UI    bool

		// SyncTimeout is the timeout for synchronous api calls
		SyncTimeout time.Duration
		Ev          interface {
			EventPublish(eventName string, data map[string]any) error
		}
	}
)

var (
	SCHEMA openapi3.T
)

func (a *Api) cdbSession() *cdb.DB {
	odb := cdb.New(a.DB)
	odb.CreateSession(a.Ev)
	return odb
}

func init() {
	if schema, err := api.GetSwagger(); err == nil {
		SCHEMA = *schema
	}
}
