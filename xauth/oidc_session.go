package xauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-redis/redis/v8"
	"golang.org/x/oauth2"
)

// loginTx is a sign-in in progress, between the redirection to the provider and
// its callback. Kept on the server side; the browser only holds its id.
type loginTx struct {
	State    string    `json:"state"`
	Nonce    string    `json:"nonce"`
	Verifier string    `json:"verifier"`
	ReturnTo string    `json:"return_to"`
	Created  time.Time `json:"created"`
}

// Session is a sign-in through the provider. The browser only holds its random id,
// in an HttpOnly cookie; Redis keys it by the hash of that id.
type Session struct {
	UserID  int64  `json:"user_id"`
	Email   string `json:"email"`
	Issuer  string `json:"iss"`
	Subject string `json:"sub"`
	SID     string `json:"sid,omitempty"`
	IDToken string `json:"id_token"`
	// Tokens are the provider's access and refresh tokens of the user, encrypted
	// (oidc_tokens.go); empty when no token key is configured.
	Tokens string `json:"tokens,omitempty"`
	// token is the provider's token CreateSession keeps, set with WithToken.
	token *oauth2.Token
	// Claims are the claims of the ID token a rule may use, shown on the Claim
	// mappings page to those who write the rules.
	Claims  map[string]any `json:"claims,omitempty"`
	Created time.Time      `json:"created"`
	// Expires is the absolute end of the session, whatever the activity.
	Expires  time.Time `json:"expires"`
	LastSeen time.Time `json:"last_seen"`
}

// Claims are the claims of a verified ID token that oc3 reads.
type Claims struct {
	Issuer            string
	Subject           string
	Email             string
	EmailVerified     bool
	PreferredUsername string
	GivenName         string
	FamilyName        string
	Name              string
	SID               string
	// token is the provider's token response, for the session to keep.
	token *oauth2.Token
	// Raw holds every claim of the ID token, for the claim rules.
	Raw map[string]any
}

// Login errors, told apart for the message the SPA shows; their details stay in
// the server log.
var (
	ErrLoginExpired = errors.New("oidc: no sign-in in progress, or it expired")
	ErrLoginState   = errors.New("oidc: state mismatch")
	ErrLoginToken   = errors.New("oidc: the provider's answer could not be verified")
)

// SessionCookieName is the name of the session cookie: __Host- prefixed, so the
// browser only accepts it Secure, on Path=/ and without Domain.
func (o *OIDC) SessionCookieName() string {
	if o.cfg.CookieSecure {
		return "__Host-oc3_session"
	}
	return "oc3_session"
}

// LoginCookieName is the name of the cookie holding the sign-in in progress.
func (o *OIDC) LoginCookieName() string {
	if o.cfg.CookieSecure {
		return "__Host-oc3_login"
	}
	return "oc3_login"
}

// LoginTTL is how long a sign-in may stay in progress.
func (o *OIDC) LoginTTL() time.Duration { return loginTTL }

// BeginLogin opens a sign-in: fresh state, nonce and PKCE verifier, kept in Redis
// for a few minutes, and the URL of the provider's authorization endpoint. It
// returns the id of the sign-in, for the cookie that will bring it back.
func (o *OIDC) BeginLogin(ctx context.Context, returnTo string) (loginID, authURL string, err error) {
	if !o.Ready() {
		return "", "", ErrNotReady
	}
	oauthCfg, _, _, _, _ := o.verifiers()
	tx := loginTx{ReturnTo: SafeReturnTo(returnTo), Created: time.Now()}
	if tx.State, err = randomToken(); err != nil {
		return "", "", err
	}
	if tx.Nonce, err = randomToken(); err != nil {
		return "", "", err
	}
	tx.Verifier = oauth2.GenerateVerifier()
	if loginID, err = randomToken(); err != nil {
		return "", "", err
	}
	b, err := json.Marshal(tx)
	if err != nil {
		return "", "", err
	}
	if err := o.redis.Set(ctx, hashKey("login", loginID), b, loginTTL).Err(); err != nil {
		return "", "", fmt.Errorf("oidc: cannot store the sign-in: %w", err)
	}
	authURL = oauthCfg.AuthCodeURL(tx.State, oidc.Nonce(tx.Nonce), oauth2.S256ChallengeOption(tx.Verifier))
	return loginID, authURL, nil
}

// consumeLogin takes the sign-in out of Redis: it can be used only once.
func (o *OIDC) consumeLogin(ctx context.Context, loginID string) (*loginTx, error) {
	if loginID == "" {
		return nil, ErrLoginExpired
	}
	b, err := o.redis.GetDel(ctx, hashKey("login", loginID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrLoginExpired
	}
	if err != nil {
		return nil, fmt.Errorf("oidc: cannot read the sign-in: %w", err)
	}
	var tx loginTx
	if err := json.Unmarshal(b, &tx); err != nil {
		return nil, ErrLoginExpired
	}
	return &tx, nil
}

// FinishLogin ends a sign-in on the provider's callback: the sign-in of the
// cookie is consumed, its state must match, the code is exchanged with the PKCE
// verifier, and the ID token is verified (signature, issuer, audience, expiry,
// nonce). It returns the claims of the token and the path to go back to.
func (o *OIDC) FinishLogin(ctx context.Context, loginID, state, code string) (*Claims, string, string, error) {
	tx, err := o.consumeLogin(ctx, loginID)
	if err != nil {
		return nil, "", "", err
	}
	if state == "" || !equalStrings(state, tx.State) {
		return nil, tx.ReturnTo, "", ErrLoginState
	}
	if !o.Ready() {
		return nil, tx.ReturnTo, "", ErrNotReady
	}
	oauthCfg, idVerifier, _, _, _ := o.verifiers()
	token, err := oauthCfg.Exchange(ctx, code, oauth2.VerifierOption(tx.Verifier))
	if err != nil {
		return nil, tx.ReturnTo, "", fmt.Errorf("%w: code exchange: %w", ErrLoginToken, err)
	}
	raw, ok := token.Extra("id_token").(string)
	if !ok || raw == "" {
		return nil, tx.ReturnTo, "", fmt.Errorf("%w: no id_token in the token response", ErrLoginToken)
	}
	idToken, err := idVerifier.Verify(ctx, raw)
	if err != nil {
		return nil, tx.ReturnTo, "", fmt.Errorf("%w: %w", ErrLoginToken, err)
	}
	if !equalStrings(idToken.Nonce, tx.Nonce) {
		return nil, tx.ReturnTo, "", fmt.Errorf("%w: nonce mismatch", ErrLoginToken)
	}
	claims, err := o.readClaims(idToken)
	if err != nil {
		return nil, tx.ReturnTo, "", fmt.Errorf("%w: %w", ErrLoginToken, err)
	}
	claims.token = token
	return claims, tx.ReturnTo, raw, nil
}

func (o *OIDC) readClaims(token *oidc.IDToken) (*Claims, error) {
	var raw map[string]any
	if err := token.Claims(&raw); err != nil {
		return nil, err
	}
	c := &Claims{Issuer: token.Issuer, Subject: token.Subject}
	if c.Subject == "" {
		return nil, errors.New("no sub claim")
	}
	c.Email, _ = raw["email"].(string)
	c.EmailVerified, _ = raw["email_verified"].(bool)
	c.PreferredUsername, _ = raw["preferred_username"].(string)
	c.GivenName, _ = raw["given_name"].(string)
	c.FamilyName, _ = raw["family_name"].(string)
	c.Name, _ = raw["name"].(string)
	c.SID, _ = raw["sid"].(string)
	c.Raw = raw
	return c, nil
}

// CreateSession stores a new session and returns its id, for the cookie. A new id
// at every sign-in: a session id known before signing in is worth nothing after.
func (o *OIDC) CreateSession(ctx context.Context, s Session) (string, error) {
	id, err := randomToken()
	if err != nil {
		return "", err
	}
	now := time.Now()
	s.Created, s.LastSeen = now, now
	if s.Claims != nil {
		s.Claims = displayedClaims(s.Claims)
	}
	s.Expires = now.Add(o.cfg.MaxLifetime)
	if s.token != nil && o.KeepsTokens() && s.token.AccessToken != "" {
		sealed, err := o.sealTokens(&s, sessionTokens{AccessToken: s.token.AccessToken, RefreshToken: s.token.RefreshToken, Expiry: s.token.Expiry})
		if err != nil {
			return "", fmt.Errorf("oidc: cannot keep the tokens: %w", err)
		}
		s.Tokens = sealed
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	key := hashKey("session", id)
	pipe := o.redis.TxPipeline()
	pipe.Set(ctx, key, b, o.cfg.IdleTimeout)
	// Indexes for the provider's back-channel logout, by session and by subject.
	for _, index := range o.sessionIndexes(s) {
		pipe.SAdd(ctx, index, key)
		pipe.Expire(ctx, index, o.cfg.MaxLifetime)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("oidc: cannot store the session: %w", err)
	}
	return id, nil
}

func (o *OIDC) sessionIndexes(s Session) []string {
	indexes := []string{hashKey("sub", s.Issuer+"\x00"+s.Subject)}
	if s.SID != "" {
		indexes = append(indexes, hashKey("sid", s.Issuer+"\x00"+s.SID))
	}
	return indexes
}

// LoadSession returns the session of this id, nil when there is none or it ended.
// An active session is extended by the idle timeout, within its absolute end; the
// write is skipped when the last one is recent.
func (o *OIDC) LoadSession(ctx context.Context, id string) (*Session, error) {
	if id == "" {
		return nil, nil
	}
	key := hashKey("session", id)
	b, err := o.redis.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: oidc session: %w", ErrUnavailable, err)
	}
	var s Session
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, nil
	}
	now := time.Now()
	if !now.Before(s.Expires) {
		_ = o.redis.Del(ctx, key).Err()
		return nil, nil
	}
	if now.Sub(s.LastSeen) >= touchInterval {
		s.LastSeen = now
		ttl := min(o.cfg.IdleTimeout, s.Expires.Sub(now))
		if b, err := json.Marshal(s); err == nil {
			_ = o.redis.Set(ctx, key, b, ttl).Err()
		}
	}
	return &s, nil
}

// DeleteSession ends the session of this id and returns it, nil if there was none.
func (o *OIDC) DeleteSession(ctx context.Context, id string) (*Session, error) {
	if id == "" {
		return nil, nil
	}
	key := hashKey("session", id)
	b, err := o.redis.GetDel(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s Session
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, nil
	}
	for _, index := range o.sessionIndexes(s) {
		_ = o.redis.SRem(ctx, index, key).Err()
	}
	return &s, nil
}

// deleteIndexed ends every session an index lists.
func (o *OIDC) deleteIndexed(ctx context.Context, index string) ([]string, error) {
	keys, err := o.redis.SMembers(ctx, index).Result()
	if err != nil {
		return nil, err
	}
	var emails []string
	if len(keys) > 0 {
		// Read before deleted, for the audit log to say whose sessions ended;
		// a session gone meanwhile is not counted.
		values, err := o.redis.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, err
		}
		for _, v := range values {
			raw, ok := v.(string)
			if !ok {
				continue
			}
			var s Session
			if json.Unmarshal([]byte(raw), &s) == nil {
				emails = append(emails, s.Email)
			}
		}
		if err := o.redis.Del(ctx, keys...).Err(); err != nil {
			return nil, err
		}
	}
	return emails, o.redis.Del(ctx, index).Err()
}

// LogoutURL is the provider's end-session URL for a session (RP-initiated
// logout), empty when the provider has none.
func (o *OIDC) LogoutURL(s *Session) string {
	_, _, _, _, endSession := o.verifiers()
	if endSession == "" {
		return ""
	}
	u, err := url.Parse(endSession)
	if err != nil {
		return ""
	}
	q := u.Query()
	q.Set("client_id", o.cfg.ClientID)
	if s != nil && s.IDToken != "" {
		q.Set("id_token_hint", s.IDToken)
	}
	if o.cfg.PostLogoutRedirectURL != "" {
		q.Set("post_logout_redirect_uri", o.cfg.PostLogoutRedirectURL)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// backchannelEvent is the event a logout token must carry.
const backchannelEvent = "http://schemas.openid.net/event/backchannel-logout"

// BackchannelLogout verifies a logout token sent by the provider (OpenID Connect
// Back-Channel Logout 1.0) and ends the sessions it names, by sid or else by sub.
// It returns the email of the user of each session ended.
func (o *OIDC) BackchannelLogout(ctx context.Context, raw string) ([]string, error) {
	if !o.Ready() {
		return nil, ErrNotReady
	}
	_, _, _, verifier, _ := o.verifiers()
	token, err := verifier.Verify(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("logout token: %w", err)
	}
	var claims struct {
		Sub    string         `json:"sub"`
		SID    string         `json:"sid"`
		Nonce  *string        `json:"nonce"`
		Events map[string]any `json:"events"`
		Exp    *int64         `json:"exp"`
	}
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("logout token claims: %w", err)
	}
	if _, ok := claims.Events[backchannelEvent]; !ok {
		return nil, errors.New("logout token: no back-channel logout event")
	}
	if claims.Nonce != nil {
		return nil, errors.New("logout token: a nonce is forbidden")
	}
	if claims.Sub == "" && claims.SID == "" {
		return nil, errors.New("logout token: neither sub nor sid")
	}
	now := time.Now()
	if token.IssuedAt.IsZero() || token.IssuedAt.After(now.Add(time.Minute)) || now.Sub(token.IssuedAt) > 10*time.Minute {
		return nil, errors.New("logout token: iat missing or not recent")
	}
	if claims.Exp != nil && now.After(time.Unix(*claims.Exp, 0).Add(time.Minute)) {
		return nil, errors.New("logout token: expired")
	}
	if claims.SID != "" {
		return o.deleteIndexed(ctx, hashKey("sid", token.Issuer+"\x00"+claims.SID))
	}
	return o.deleteIndexed(ctx, hashKey("sub", token.Issuer+"\x00"+claims.Sub))
}

// SameOrigin tells whether a request made with a session cookie comes from the
// SPA's origin. Origin is compared when the browser sends it; otherwise
// Sec-Fetch-Site, when present, must say same-origin. A request with neither is
// left to the CSRF header, which only same-origin JavaScript can set.
func (o *OIDC) SameOrigin(origin, fetchSite string) bool {
	if origin != "" && origin != "null" {
		return strings.EqualFold(strings.TrimSuffix(origin, "/"), o.origin)
	}
	if fetchSite != "" {
		return fetchSite == "same-origin"
	}
	return origin != "null"
}
