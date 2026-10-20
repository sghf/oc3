package serverhandlers

import "github.com/labstack/echo/v4"

// actionActor is who queues an agent action: the caller of PUT /actions, or the
// user a workflow run acts for. The queueing checks the privileges and the
// responsibilities of this user, and the queue and the audit log name it.
type actionActor struct {
	userID *int64
	email  string
	groups []string

	// dryRun runs every check of the queueing but queues nothing: the action is
	// returned with id 0, as it would have been queued.
	dryRun bool
}

// actorOf is the authenticated caller of a request.
func actorOf(c echo.Context) actionActor {
	email, _ := c.Get(XUserEmail).(string)
	return actionActor{userID: authUserID(c), email: email, groups: UserGroupsFromContext(c)}
}

func (a actionActor) has(group string) bool {
	for _, g := range a.groups {
		if g == group {
			return true
		}
	}
	return false
}

func (a actionActor) manager() bool { return a.has("Manager") }
