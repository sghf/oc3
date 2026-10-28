package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// CSRFHeader is the header the SPA sets on the requests that change something.
// Only JavaScript of the same origin can set it: a form of another site cannot,
// and without CORS a cross-site fetch cannot either.
const CSRFHeader = "X-OC3-CSRF"

// The codes the callback sends back to the SPA in auth_error when a sign-in fails.
// The details stay in the server log: the browser only learns the kind of failure.
const (
	authErrorDenied      = "denied"
	authErrorExpired     = "expired"
	authErrorState       = "state"
	authErrorToken       = "token"
	authErrorUnknownUser = "unknown_user"
	// authErrorNotAllowed: the account would be created, but the identity is not
	// in a group allowed to use the collector.
	authErrorNotAllowed  = "not_allowed"
	authErrorLocked      = "locked"
	authErrorUnavailable = "unavailable"
)

func noStore(c echo.Context) {
	c.Response().Header().Set("Cache-Control", "no-store")
	c.Response().Header().Set("Pragma", "no-cache")
}

// setAuthCookie sets an HttpOnly cookie on the whole origin, Lax so that it comes
// back with the provider's redirection, Secure unless configured for plain http.
// maxAge 0 clears it.
func (a *Api) setAuthCookie(c echo.Context, name, value string, maxAge int) {
	cookie := &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   a.OIDC.Config().CookieSecure,
		SameSite: http.SameSiteLaxMode,
	}
	if maxAge > 0 {
		cookie.MaxAge = maxAge
	} else {
		cookie.MaxAge = -1
		cookie.Value = ""
	}
	c.SetCookie(cookie)
}

// GetAuthInfo handles GET /auth/info: the sign-in modes, for the sign-in screen,
// and the issuer and client id of the OpenID Connect provider.
func (a *Api) GetAuthInfo(c echo.Context) error {
	noStore(c)
	info := server.AuthInfo{Basic: a.BasicUsers}
	if a.OIDC != nil {
		info.Oidc.Enabled = true
		info.Oidc.Ready = a.OIDC.Ready()
		info.Oidc.Name = a.OIDC.Config().DisplayName
		// Both public: the discovery document gives the issuer, and the client id
		// shows in the authorize URL of every sign-in.
		info.Oidc.Issuer = a.OIDC.Config().Issuer
		info.Oidc.ClientId = a.OIDC.Config().ClientID
		if cookie, err := c.Cookie(a.OIDC.SessionCookieName()); err == nil {
			if sess, err := a.OIDC.LoadSession(c.Request().Context(), cookie.Value); err == nil && sess != nil {
				info.Session = true
			}
		}
	}
	return c.JSON(http.StatusOK, info)
}

// GetAuthLogin handles GET /auth/login: the start of an OpenID Connect sign-in.
func (a *Api) GetAuthLogin(c echo.Context, params server.GetAuthLoginParams) error {
	log := echolog.GetLogHandler(c, "GetAuthLogin")
	noStore(c)
	if a.OIDC == nil {
		return JSONProblem(c, http.StatusNotFound, "OpenID Connect sign-in is not enabled")
	}
	returnTo := ""
	if params.ReturnTo != nil {
		returnTo = *params.ReturnTo
	}
	loginID, authURL, err := a.OIDC.BeginLogin(c.Request().Context(), returnTo)
	if errors.Is(err, xauth.ErrNotReady) {
		return JSONProblem(c, http.StatusServiceUnavailable, "the identity provider is not reachable yet; retry later")
	}
	if err != nil {
		log.Error("cannot start the sign-in", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot start the sign-in")
	}
	a.setAuthCookie(c, a.OIDC.LoginCookieName(), loginID, int(a.OIDC.LoginTTL().Seconds()))
	return c.Redirect(http.StatusFound, authURL)
}

// withAuthError adds the failure code of a sign-in to the path the SPA goes back to.
func withAuthError(returnTo, code string) string {
	u, err := url.Parse(xauth.SafeReturnTo(returnTo))
	if err != nil {
		return "/?auth_error=" + url.QueryEscape(code)
	}
	q := u.Query()
	q.Set("auth_error", code)
	u.RawQuery = q.Encode()
	return u.String()
}

// GetAuthCallback handles GET /auth/callback: the end of an OpenID Connect
// sign-in. It always redirects back to the SPA, with auth_error on failure.
func (a *Api) GetAuthCallback(c echo.Context, params server.GetAuthCallbackParams) error {
	log := echolog.GetLogHandler(c, "GetAuthCallback")
	noStore(c)
	if a.OIDC == nil {
		return JSONProblem(c, http.StatusNotFound, "OpenID Connect sign-in is not enabled")
	}
	ctx := c.Request().Context()
	loginID := ""
	if cookie, err := c.Cookie(a.OIDC.LoginCookieName()); err == nil {
		loginID = cookie.Value
	}
	// The sign-in in progress is used once, whatever the outcome.
	a.setAuthCookie(c, a.OIDC.LoginCookieName(), "", 0)

	if params.Error != nil && *params.Error != "" {
		// The provider refused or the user cancelled: the sign-in in progress goes.
		_, _, _, _ = a.OIDC.FinishLogin(ctx, loginID, "", "")
		log.Info("sign-in refused by the provider", "error", *params.Error)
		return c.Redirect(http.StatusSeeOther, withAuthError("/", authErrorDenied))
	}
	code, state := "", ""
	if params.Code != nil {
		code = *params.Code
	}
	if params.State != nil {
		state = *params.State
	}
	claims, returnTo, idToken, err := a.OIDC.FinishLogin(ctx, loginID, state, code)
	if err != nil {
		reason := authErrorToken
		switch {
		case errors.Is(err, xauth.ErrLoginExpired):
			reason = authErrorExpired
		case errors.Is(err, xauth.ErrLoginState):
			reason = authErrorState
		case errors.Is(err, xauth.ErrNotReady):
			reason = authErrorUnavailable
		}
		log.Warn("sign-in failed", "reason", reason, logkey.Error, err)
		return c.Redirect(http.StatusSeeOther, withAuthError(returnTo, reason))
	}

	user, reason, err := a.resolveOIDCUser(ctx, log, claims)
	if err != nil {
		log.Error("cannot resolve the account of an identity", "iss", claims.Issuer, "sub", claims.Subject, logkey.Error, err)
		return c.Redirect(http.StatusSeeOther, withAuthError(returnTo, authErrorUnavailable))
	}
	if reason != "" {
		return c.Redirect(http.StatusSeeOther, withAuthError(returnTo, reason))
	}

	session := xauth.Session{
		UserID:  user.ID,
		Email:   user.Email,
		Issuer:  claims.Issuer,
		Subject: claims.Subject,
		SID:     claims.SID,
		IDToken: idToken,
		Claims:  claims.Raw,
	}
	// The provider's tokens, kept encrypted when a token key is configured, for
	// the services oc3 calls on the user's behalf (the AI assistant).
	session.WithToken(claims.Token())
	sessionID, err := a.OIDC.CreateSession(ctx, session)
	if err != nil {
		log.Error("cannot open a session", logkey.Error, err)
		return c.Redirect(http.StatusSeeOther, withAuthError(returnTo, authErrorUnavailable))
	}
	a.setAuthCookie(c, a.OIDC.SessionCookieName(), sessionID, int(a.OIDC.Config().MaxLifetime.Seconds()))
	if logErr := a.ODB.Log(ctx, cdb.LogEntry{
		Action: "user.login",
		User:   user.Email,
		Fmt:    "signed in through %(provider)s",
		Dict:   map[string]any{"provider": a.OIDC.Config().DisplayName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
	log.Info("signed in", "user_id", user.ID, "iss", claims.Issuer)
	return c.Redirect(http.StatusSeeOther, returnTo)
}

// resolveOIDCUser returns the account of a verified identity, linking or creating
// it as configured, and aligns its mapped roles on the provider's groups. A
// non-empty reason is the auth_error to send the SPA when there is no account to
// sign in.
func (a *Api) resolveOIDCUser(ctx context.Context, log *slog.Logger, claims *xauth.Claims) (*cdb.IdentityUser, string, error) {
	cfg := a.OIDC.Config()
	odb := a.ODB
	user, err := odb.UserByIdentity(ctx, claims.Issuer, claims.Subject)
	if err != nil {
		return nil, "", err
	}
	email := strings.TrimSpace(claims.Email)

	// The claim rules decide who may sign in, account or not, and which of the
	// teams they name the account belongs to.
	rules, err := a.OIDC.ClaimRules(ctx)
	if err != nil {
		return nil, "", err
	}
	outcome := xauth.EvaluateClaimRules(rules, claims.Raw)
	if !outcome.MayAccess() {
		log.Warn("sign-in refused: no claim rule allows this identity", "iss", claims.Issuer, "sub", claims.Subject)
		return nil, authErrorNotAllowed, nil
	}

	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("cannot start transaction: %w", err)
	}
	defer endTx()

	if user == nil {
		var existing *cdb.IdentityUser
		if email != "" {
			if existing, err = tx.UserByEmailForIdentity(ctx, email); err != nil {
				return nil, "", err
			}
		}
		switch {
		case existing != nil && cfg.LinkByVerifiedEmail && claims.EmailVerified:
			if existing.Locked {
				log.Warn("sign-in of a locked account refused", "user_id", existing.ID)
				return nil, authErrorLocked, nil
			}
			if err := tx.LinkIdentity(ctx, existing.ID, claims.Issuer, claims.Subject); err != nil {
				return nil, "", err
			}
			if logErr := tx.Log(ctx, cdb.LogEntry{
				Action: "users.identity.link",
				User:   existing.Email,
				Fmt:    "linked the %(provider)s identity %(sub)s to the account %(email)s",
				Dict:   map[string]any{"provider": cfg.DisplayName, "sub": claims.Subject, "email": existing.Email},
				Level:  "info",
			}); logErr != nil {
				log.Error("cannot write audit log", logkey.Error, logErr)
			}
			user = existing
		case existing != nil:
			// An account has this email, but the provider does not vouch for it or
			// linking is off: taking it over would let anyone who sets that email
			// at the provider sign in as its owner.
			log.Warn("sign-in of an identity not linked to any account refused",
				"iss", claims.Issuer, "sub", claims.Subject,
				"email_verified", claims.EmailVerified, "link_by_verified_email", cfg.LinkByVerifiedEmail)
			return nil, authErrorUnknownUser, nil
		case email != "" && cfg.AutoCreateUsers && !outcome.MayCreate():
			// Creation needs a rule allowing the access explicitly: without access
			// rules, nobody gets an account by default.
			log.Warn("first sign-in refused: no claim rule allows creating this account", "iss", claims.Issuer, "sub", claims.Subject)
			return nil, authErrorNotAllowed, nil
		case email != "" && cfg.AutoCreateUsers:
			insert := cdb.UserInsert{Email: email}
			if claims.GivenName != "" {
				insert.FirstName = &claims.GivenName
			}
			if claims.FamilyName != "" {
				insert.LastName = &claims.FamilyName
			}
			if name := claims.PreferredUsername; name != "" {
				if _, taken, err := tx.UserIDByUsername(ctx, name); err != nil {
					return nil, "", err
				} else if !taken {
					insert.Username = &name
				}
			}
			id, err := tx.InsertUserWithPrivateGroup(ctx, insert)
			if err != nil {
				return nil, "", err
			}
			if err := tx.LinkIdentity(ctx, id, claims.Issuer, claims.Subject); err != nil {
				return nil, "", err
			}
			if logErr := tx.Log(ctx, cdb.LogEntry{
				Action: "user.create",
				User:   email,
				Fmt:    "add user %(email)s at its first sign-in through %(provider)s",
				Dict:   map[string]any{"email": email, "provider": cfg.DisplayName},
				Level:  "info",
			}); logErr != nil {
				log.Error("cannot write audit log", logkey.Error, logErr)
			}
			user = &cdb.IdentityUser{ID: id, Email: email}
		default:
			log.Warn("sign-in of an identity unknown to the collector refused", "iss", claims.Issuer, "sub", claims.Subject)
			return nil, authErrorUnknownUser, nil
		}
	} else {
		if user.Locked {
			log.Warn("sign-in of a locked account refused", "user_id", user.ID)
			return nil, authErrorLocked, nil
		}
		if err := tx.TouchIdentity(ctx, claims.Issuer, claims.Subject); err != nil {
			return nil, "", err
		}
		if err := tx.UpdateUserNames(ctx, user.ID, claims.GivenName, claims.FamilyName); err != nil {
			return nil, "", err
		}
	}

	joined, left, err := tx.SyncMappedGroups(ctx, user.ID, outcome.ManagedIDs, outcome.GrantedIDs)
	if err != nil {
		return nil, "", err
	}
	if len(joined) > 0 || len(left) > 0 {
		if logErr := tx.Log(ctx, cdb.LogEntry{
			Action: "users.groups.sync",
			User:   user.Email,
			Fmt:    "aligned the teams of %(email)s on %(provider)s: joined %(joined)s, left %(left)s",
			Dict: map[string]any{
				"email": user.Email, "provider": cfg.DisplayName,
				"joined": strings.Join(joined, ", "), "left": strings.Join(left, ", "),
			},
			Level: "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
	}
	markSuccess()
	return user, "", nil
}

// sameOriginRequest tells whether a request that relies on the session cookie
// comes from the SPA: the CSRF header is set and the origin is the SPA's.
func (a *Api) sameOriginRequest(r *http.Request) bool {
	return r.Header.Get(CSRFHeader) == "1" &&
		a.OIDC.SameOrigin(r.Header.Get("Origin"), r.Header.Get("Sec-Fetch-Site"))
}

// PostAuthLogout handles POST /auth/logout: the end of the OpenID Connect session
// of the request, and the provider's end-session URL for the SPA to follow.
func (a *Api) PostAuthLogout(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostAuthLogout")
	noStore(c)
	if a.OIDC == nil {
		return c.JSON(http.StatusOK, map[string]string{"logout_url": ""})
	}
	if !a.sameOriginRequest(c.Request()) {
		return JSONProblem(c, http.StatusForbidden, "cross-site request refused")
	}
	ctx := c.Request().Context()
	var sess *xauth.Session
	if cookie, err := c.Cookie(a.OIDC.SessionCookieName()); err == nil {
		var err error
		if sess, err = a.OIDC.DeleteSession(ctx, cookie.Value); err != nil {
			log.Error("cannot delete the session", logkey.Error, err)
		}
	}
	a.setAuthCookie(c, a.OIDC.SessionCookieName(), "", 0)
	if sess != nil {
		if logErr := a.ODB.Log(ctx, cdb.LogEntry{
			Action: "user.logout",
			User:   sess.Email,
			Fmt:    "signed out",
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
	}
	return c.JSON(http.StatusOK, map[string]string{"logout_url": a.OIDC.LogoutURL(sess)})
}

// PostAuthBackchannelLogout handles POST /auth/backchannel-logout: the provider
// ends sessions, authenticated by the signature of its logout token.
func (a *Api) PostAuthBackchannelLogout(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostAuthBackchannelLogout")
	noStore(c)
	if a.OIDC == nil {
		return JSONProblem(c, http.StatusNotFound, "OpenID Connect sign-in is not enabled")
	}
	ctx := c.Request().Context()
	emails, err := a.OIDC.BackchannelLogout(ctx, c.FormValue("logout_token"))
	if errors.Is(err, xauth.ErrNotReady) {
		return JSONProblem(c, http.StatusServiceUnavailable, "the identity provider is not reachable yet")
	}
	if err != nil {
		log.Warn("back-channel logout refused", logkey.Error, err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid_request"})
	}
	log.Info("back-channel logout", "sessions", len(emails))
	// Each user whose sessions the provider ended, once: signed out as by
	// POST /auth/logout, by the provider rather than the user.
	seen := map[string]bool{}
	for _, email := range emails {
		if seen[email] {
			continue
		}
		seen[email] = true
		if logErr := a.ODB.Log(ctx, cdb.LogEntry{
			Action: "user.logout",
			User:   email,
			Fmt:    "signed out by the identity provider (back-channel logout)",
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
	}
	return c.NoContent(http.StatusOK)
}

// CSRFMiddleware refuses the requests authenticated by the session cookie that
// change something without coming from the SPA: the CSRF header is missing, or
// the Origin is another site's. Requests with a Bearer token or a password are
// not concerned: a browser never sends those on its own.
func CSRFMiddleware(o *xauth.OIDC) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if o == nil {
				return next(c)
			}
			switch c.Request().Method {
			case http.MethodGet, http.MethodHead, http.MethodOptions:
				return next(c)
			}
			user := UserInfoFromContext(c)
			if user == nil || user.GetExtensions().Get(xauth.XAuthSource) != xauth.AuthSourceSession {
				return next(c)
			}
			r := c.Request()
			if r.Header.Get(CSRFHeader) != "1" || !o.SameOrigin(r.Header.Get("Origin"), r.Header.Get("Sec-Fetch-Site")) {
				return JSONProblem(c, http.StatusForbidden, "cross-site request refused")
			}
			return next(c)
		}
	}
}
