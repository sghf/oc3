package xauth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-redis/redis/v8"
)

// fakeProvider is an OpenID Connect provider for the tests: discovery, JWKS, and a
// token endpoint that checks the PKCE verifier of the code it issued.
type fakeProvider struct {
	t      *testing.T
	srv    *httptest.Server
	key    *rsa.PrivateKey
	kid    string
	client string

	mu    sync.Mutex
	codes map[string]pendingCode
	// idClaims are added to the ID tokens the token endpoint issues.
	idClaims map[string]any
	// refreshTokens are the refresh tokens still valid; refreshes counts the
	// refresh grants served. Each refresh rotates the refresh token.
	refreshTokens map[string]bool
	refreshes     int
}

type pendingCode struct {
	challenge string
	nonce     string
}

func newFakeProvider(t *testing.T) *fakeProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProvider{t: t, key: key, kid: "k1", client: "oc3", codes: map[string]pendingCode{}, idClaims: map[string]any{}, refreshTokens: map[string]bool{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                p.srv.URL,
			"authorization_endpoint":                p.srv.URL + "/authorize",
			"token_endpoint":                        p.srv.URL + "/token",
			"jwks_uri":                              p.srv.URL + "/jwks",
			"end_session_endpoint":                  p.srv.URL + "/end-session",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
			{Key: &p.key.PublicKey, KeyID: p.kid, Algorithm: "RS256", Use: "sig"},
		}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("grant_type") == "refresh_token" {
			p.mu.Lock()
			defer p.mu.Unlock()
			old := r.Form.Get("refresh_token")
			if !p.refreshTokens[old] {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
				return
			}
			delete(p.refreshTokens, old)
			p.refreshes++
			next := fmt.Sprintf("refresh-%d", p.refreshes)
			p.refreshTokens[next] = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": fmt.Sprintf("access-%d", p.refreshes), "token_type": "Bearer", "expires_in": 300, "refresh_token": next,
			})
			return
		}
		p.mu.Lock()
		pending, ok := p.codes[r.Form.Get("code")]
		delete(p.codes, r.Form.Get("code"))
		p.mu.Unlock()
		sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if !ok || base64.RawURLEncoding.EncodeToString(sum[:]) != pending.challenge {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		claims := map[string]any{
			"iss": p.srv.URL, "aud": p.client, "sub": "user-1",
			"iat": time.Now().Unix(), "exp": time.Now().Add(5 * time.Minute).Unix(),
			"nonce": pending.nonce, "email": "alice@example.com", "email_verified": true,
			"groups": []string{"admins", "ops"}, "sid": "sid-1",
		}
		for k, v := range p.idClaims {
			claims[k] = v
		}
		p.mu.Lock()
		p.refreshTokens["refresh-0"] = true
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "opaque", "token_type": "Bearer", "expires_in": 300, "refresh_token": "refresh-0",
			"id_token": p.sign(claims, "RS256"),
		})
	})
	p.srv = httptest.NewServer(mux)
	t.Cleanup(p.srv.Close)
	return p
}

func (p *fakeProvider) sign(claims map[string]any, alg string) string {
	p.t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.SignatureAlgorithm(alg), Key: jose.JSONWebKey{Key: p.key, KeyID: p.kid}}, nil)
	if err != nil {
		p.t.Fatal(err)
	}
	b, _ := json.Marshal(claims)
	jws, err := signer.Sign(b)
	if err != nil {
		p.t.Fatal(err)
	}
	s, err := jws.CompactSerialize()
	if err != nil {
		p.t.Fatal(err)
	}
	return s
}

// authorize plays the provider's authorization endpoint: it reads the request
// the client built and issues a code bound to its PKCE challenge and nonce.
func (p *fakeProvider) authorize(t *testing.T, authURL string) (code, state string) {
	t.Helper()
	u, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" ||
		q.Get("code_challenge") == "" || q.Get("nonce") == "" || q.Get("state") == "" ||
		q.Get("client_id") != p.client {
		t.Fatalf("unexpected authorization request %s", authURL)
	}
	code = "code-" + q.Get("state")[:8]
	p.mu.Lock()
	p.codes[code] = pendingCode{challenge: q.Get("code_challenge"), nonce: q.Get("nonce")}
	p.mu.Unlock()
	return code, q.Get("state")
}

func newTestOIDC(t *testing.T, p *fakeProvider, edit func(*OIDCConfig)) (*OIDC, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := OIDCConfig{
		Enable: true, Issuer: p.srv.URL, ClientID: p.client, ClientSecret: "s3cret",
		RedirectURL:  "https://collector.example.com/api/auth/callback",
		CookieSecure: true,
	}
	if edit != nil {
		edit(&cfg)
	}
	o, err := NewOIDC(context.Background(), cfg, rdb, nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !o.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("provider not discovered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return o, mr
}

func TestLoginFlow(t *testing.T) {
	p := newFakeProvider(t)
	o, _ := newTestOIDC(t, p, nil)
	ctx := context.Background()

	loginID, authURL, err := o.BeginLogin(ctx, "/nodes?sel=1")
	if err != nil {
		t.Fatal(err)
	}
	code, state := p.authorize(t, authURL)
	claims, returnTo, idToken, err := o.FinishLogin(ctx, loginID, state, code)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "user-1" || claims.Email != "alice@example.com" || !claims.EmailVerified || claims.SID != "sid-1" {
		t.Fatalf("unexpected claims %+v", claims)
	}
	if returnTo != "/nodes?sel=1" || idToken == "" {
		t.Fatalf("unexpected returnTo %q or empty id token", returnTo)
	}
	// The sign-in in progress is used once.
	if _, _, _, err := o.FinishLogin(ctx, loginID, state, code); !errors.Is(err, ErrLoginExpired) {
		t.Fatalf("replayed sign-in: want ErrLoginExpired, got %v", err)
	}
}

func TestLoginRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("state mismatch", func(t *testing.T) {
		p := newFakeProvider(t)
		o, _ := newTestOIDC(t, p, nil)
		loginID, authURL, _ := o.BeginLogin(ctx, "/")
		code, _ := p.authorize(t, authURL)
		if _, _, _, err := o.FinishLogin(ctx, loginID, "forged", code); !errors.Is(err, ErrLoginState) {
			t.Fatalf("want ErrLoginState, got %v", err)
		}
	})
	t.Run("unknown sign-in", func(t *testing.T) {
		p := newFakeProvider(t)
		o, _ := newTestOIDC(t, p, nil)
		if _, _, _, err := o.FinishLogin(ctx, "nope", "s", "c"); !errors.Is(err, ErrLoginExpired) {
			t.Fatalf("want ErrLoginExpired, got %v", err)
		}
	})
	t.Run("nonce mismatch", func(t *testing.T) {
		p := newFakeProvider(t)
		p.idClaims["nonce"] = "other"
		o, _ := newTestOIDC(t, p, nil)
		loginID, authURL, _ := o.BeginLogin(ctx, "/")
		code, state := p.authorize(t, authURL)
		if _, _, _, err := o.FinishLogin(ctx, loginID, state, code); !errors.Is(err, ErrLoginToken) {
			t.Fatalf("want ErrLoginToken, got %v", err)
		}
	})
	t.Run("wrong audience", func(t *testing.T) {
		p := newFakeProvider(t)
		p.idClaims["aud"] = "another-client"
		o, _ := newTestOIDC(t, p, nil)
		loginID, authURL, _ := o.BeginLogin(ctx, "/")
		code, state := p.authorize(t, authURL)
		if _, _, _, err := o.FinishLogin(ctx, loginID, state, code); !errors.Is(err, ErrLoginToken) {
			t.Fatalf("want ErrLoginToken, got %v", err)
		}
	})
	t.Run("expired id token", func(t *testing.T) {
		p := newFakeProvider(t)
		p.idClaims["exp"] = time.Now().Add(-time.Hour).Unix()
		o, _ := newTestOIDC(t, p, nil)
		loginID, authURL, _ := o.BeginLogin(ctx, "/")
		code, state := p.authorize(t, authURL)
		if _, _, _, err := o.FinishLogin(ctx, loginID, state, code); !errors.Is(err, ErrLoginToken) {
			t.Fatalf("want ErrLoginToken, got %v", err)
		}
	})
	t.Run("pkce verifier of another sign-in", func(t *testing.T) {
		p := newFakeProvider(t)
		o, _ := newTestOIDC(t, p, nil)
		loginA, urlA, _ := o.BeginLogin(ctx, "/")
		_, urlB, _ := o.BeginLogin(ctx, "/")
		_, stateA := p.authorize(t, urlA)
		codeB, _ := p.authorize(t, urlB)
		// The code of B redeemed with the verifier of A: the provider refuses it.
		if _, _, _, err := o.FinishLogin(ctx, loginA, stateA, codeB); !errors.Is(err, ErrLoginToken) {
			t.Fatalf("want ErrLoginToken, got %v", err)
		}
	})
}

func TestAccessTokens(t *testing.T) {
	p := newFakeProvider(t)
	o, _ := newTestOIDC(t, p, nil)
	ctx := context.Background()
	base := func() map[string]any {
		return map[string]any{
			"iss": p.srv.URL, "aud": "oc3", "sub": "user-1",
			"iat": time.Now().Unix(), "exp": time.Now().Add(5 * time.Minute).Unix(),
			"groups": []string{"admins"},
		}
	}

	tok, err := o.VerifyAccessToken(ctx, p.sign(base(), "RS256"))
	if err != nil {
		t.Fatalf("valid token refused: %v", err)
	}
	if tok.subject != "user-1" || len(ClaimValues(tok.claims, "groups")) != 1 {
		t.Fatalf("unexpected token %+v", tok)
	}

	refused := map[string]string{}
	c := base()
	c["exp"] = time.Now().Add(-time.Minute).Unix()
	refused["expired"] = p.sign(c, "RS256")
	c = base()
	c["aud"] = "another-api"
	refused["wrong audience"] = p.sign(c, "RS256")
	c = base()
	c["iss"] = "https://evil.example.com"
	refused["wrong issuer"] = p.sign(c, "RS256")
	c = base()
	delete(c, "exp")
	refused["no exp"] = p.sign(c, "RS256")

	// alg none: a header and a payload, no signature.
	payload, _ := json.Marshal(base())
	refused["alg none"] = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","kid":"k1"}`)) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "."

	// HS256 keyed with the public key, the classic confusion attack.
	pub, _ := x509.MarshalPKIXPublicKey(&p.key.PublicKey)
	hs, _ := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: pub}, nil)
	jws, _ := hs.Sign(payload)
	refused["hs256 with the public key"], _ = jws.CompactSerialize()

	// Signed by a key the provider does not publish.
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	foreign, _ := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: other, KeyID: "unknown"}}, nil)
	jws, _ = foreign.Sign(payload)
	refused["unknown key"], _ = jws.CompactSerialize()

	for name, raw := range refused {
		if _, err := o.VerifyAccessToken(ctx, raw); err == nil {
			t.Errorf("%s: token accepted", name)
		}
	}
}

func TestSessions(t *testing.T) {
	p := newFakeProvider(t)
	o, mr := newTestOIDC(t, p, func(c *OIDCConfig) {
		c.IdleTimeout = time.Hour
		c.MaxLifetime = 2 * time.Hour
	})
	ctx := context.Background()

	id, err := o.CreateSession(ctx, Session{UserID: 7, Email: "a@example.com", Issuer: p.srv.URL, Subject: "user-1", SID: "sid-1"})
	if err != nil {
		t.Fatal(err)
	}
	// Only hashes reach Redis, never the id the cookie carries.
	for _, key := range mr.Keys() {
		if strings.Contains(key, id) {
			t.Fatalf("session id stored in clear in key %s", key)
		}
	}
	s, err := o.LoadSession(ctx, id)
	if err != nil || s == nil || s.UserID != 7 {
		t.Fatalf("load: %+v %v", s, err)
	}
	if s, _ := o.LoadSession(ctx, "forged"); s != nil {
		t.Fatal("forged session id accepted")
	}
	// Idle timeout.
	mr.FastForward(61 * time.Minute)
	if s, _ := o.LoadSession(ctx, id); s != nil {
		t.Fatal("idle session still valid")
	}

	// Explicit logout.
	id, _ = o.CreateSession(ctx, Session{UserID: 7, Issuer: p.srv.URL, Subject: "user-1"})
	if s, _ := o.DeleteSession(ctx, id); s == nil {
		t.Fatal("logout found no session")
	}
	if s, _ := o.LoadSession(ctx, id); s != nil {
		t.Fatal("session alive after logout")
	}

	// RP-initiated logout URL.
	u := o.LogoutURL(&Session{IDToken: "the-id-token"})
	if !strings.HasPrefix(u, p.srv.URL+"/end-session?") || !strings.Contains(u, "id_token_hint=the-id-token") {
		t.Fatalf("unexpected logout url %s", u)
	}
}

func TestBackchannelLogout(t *testing.T) {
	p := newFakeProvider(t)
	o, _ := newTestOIDC(t, p, nil)
	ctx := context.Background()
	logoutToken := func(edit func(map[string]any)) string {
		c := map[string]any{
			"iss": p.srv.URL, "aud": "oc3", "iat": time.Now().Unix(), "jti": "j1",
			"sub": "user-1", "sid": "sid-1",
			"events": map[string]any{backchannelEvent: map[string]any{}},
		}
		if edit != nil {
			edit(c)
		}
		return p.sign(c, "RS256")
	}

	a, _ := o.CreateSession(ctx, Session{UserID: 1, Email: "user-1@example.com", Issuer: p.srv.URL, Subject: "user-1", SID: "sid-1"})
	b, _ := o.CreateSession(ctx, Session{UserID: 1, Issuer: p.srv.URL, Subject: "user-1", SID: "sid-2"})

	for name, raw := range map[string]string{
		"with a nonce":  logoutToken(func(c map[string]any) { c["nonce"] = "n" }),
		"without event": logoutToken(func(c map[string]any) { delete(c, "events") }),
		"old":           logoutToken(func(c map[string]any) { c["iat"] = time.Now().Add(-time.Hour).Unix() }),
		"no sub no sid": logoutToken(func(c map[string]any) { delete(c, "sub"); delete(c, "sid") }),
		"other client":  logoutToken(func(c map[string]any) { c["aud"] = "another" }),
	} {
		if _, err := o.BackchannelLogout(ctx, raw); err == nil {
			t.Errorf("%s: logout token accepted", name)
		}
	}

	// By sid: only that sign-in ends.
	if emails, err := o.BackchannelLogout(ctx, logoutToken(nil)); err != nil || len(emails) != 1 || emails[0] != "user-1@example.com" {
		t.Fatalf("by sid: %v %v", emails, err)
	}
	if s, _ := o.LoadSession(ctx, a); s != nil {
		t.Fatal("session of sid-1 still alive")
	}
	if s, _ := o.LoadSession(ctx, b); s == nil {
		t.Fatal("session of sid-2 ended by the logout of sid-1")
	}
	// By sub: every sign-in of the user ends.
	if emails, err := o.BackchannelLogout(ctx, logoutToken(func(c map[string]any) { delete(c, "sid") })); err != nil || len(emails) != 1 {
		t.Fatalf("by sub: %v %v", emails, err)
	}
	if s, _ := o.LoadSession(ctx, b); s != nil {
		t.Fatal("session of the user still alive after logout by sub")
	}
}

func TestSafeReturnTo(t *testing.T) {
	for in, want := range map[string]string{
		"":                         "/",
		"/nodes?sel=1":             "/nodes?sel=1",
		"//evil.example.com":       "/",
		"/\\evil.example.com":      "/",
		"https://evil.example.com": "/",
		"javascript:alert(1)":      "/",
		"nodes":                    "/",
		"/ok\r\nSet-Cookie: x=1":   "/",
	} {
		if got := SafeReturnTo(in); got != want {
			t.Errorf("SafeReturnTo(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSameOrigin(t *testing.T) {
	p := newFakeProvider(t)
	o, _ := newTestOIDC(t, p, nil)
	cases := []struct {
		origin, site string
		want         bool
	}{
		{"https://collector.example.com", "same-origin", true},
		{"https://collector.example.com", "", true},
		{"https://evil.example.com", "", false},
		{"null", "", false},
		{"", "cross-site", false},
		{"", "same-origin", true},
		{"", "", true},
	}
	for _, c := range cases {
		if got := o.SameOrigin(c.origin, c.site); got != c.want {
			t.Errorf("SameOrigin(%q, %q) = %v, want %v", c.origin, c.site, got, c.want)
		}
	}
}

func TestConfigRefusals(t *testing.T) {
	p := newFakeProvider(t)
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	base := OIDCConfig{Enable: true, Issuer: p.srv.URL, ClientID: "oc3", ClientSecret: "s", RedirectURL: "https://c.example.com/api/auth/callback", CookieSecure: true}
	for name, edit := range map[string]func(*OIDCConfig){
		"no secret":              func(c *OIDCConfig) { c.ClientSecret = "" },
		"insecure cookie on tls": func(c *OIDCConfig) { c.CookieSecure = false },
		"relative redirect":      func(c *OIDCConfig) { c.RedirectURL = "/api/auth/callback" },
	} {
		cfg := base
		edit(&cfg)
		if _, err := NewOIDC(context.Background(), cfg, rdb, nil); err == nil {
			t.Errorf("%s: configuration accepted", name)
		}
	}
}

func TestClaimValues(t *testing.T) {
	claims := map[string]any{
		"groups":                          []any{"opensvc-collector", "ops", 42.0},
		"email":                           "alice@example.com",
		"email_verified":                  true,
		"level":                           3.0,
		"realm_access":                    map[string]any{"roles": []any{"admin", "viewer"}},
		"https://example.com/claims/team": "storage",
	}
	cases := []struct {
		name, value string
		want        bool
	}{
		{"groups", "opensvc-collector", true},
		{"groups", "OpenSVC-Collector", false},
		{"groups", "42", true},
		{"email", "alice@example.com", true},
		{"email", "alice", false},
		{"email_verified", "true", true},
		{"level", "3", true},
		{"realm_access.roles", "admin", true},
		{"realm_access.roles", "root", false},
		{"realm_access.missing", "admin", false},
		{"https://example.com/claims/team", "storage", true},
		{"absent", "x", false},
	}
	for _, c := range cases {
		if got := ClaimMatches(claims, c.name, c.value); got != c.want {
			t.Errorf("ClaimMatches(%q, %q) = %v, want %v", c.name, c.value, got, c.want)
		}
	}
}

func TestEvaluateClaimRules(t *testing.T) {
	member := map[string]any{"groups": []any{"opensvc-collector", "admins"}}
	stranger := map[string]any{"groups": []any{"marketing"}}

	// Without access rules, anyone may sign in, but nobody is created.
	none := EvaluateClaimRules(nil, stranger)
	if !none.MayAccess() || none.MayCreate() {
		t.Fatalf("no rules: access %v create %v", none.MayAccess(), none.MayCreate())
	}

	rules := []ClaimRule{
		{ID: 1, Claim: "groups", Value: "opensvc-collector", AllowAccess: true},
		// One claim value granting several teams.
		{ID: 2, Claim: "groups", Value: "admins", GroupIDs: []int64{10, 12}, GroupRoles: []string{"Manager", "NodeManager"}},
		{ID: 3, Claim: "groups", Value: "netops", GroupIDs: []int64{11}, GroupRoles: []string{"NetworkManager"}},
		{ID: 4, Claim: "groups", Value: "ops", GroupIDs: []int64{11, 12}, GroupRoles: []string{"NetworkManager", "NodeManager"}},
	}
	m := EvaluateClaimRules(rules, member)
	if !m.MayAccess() || !m.MayCreate() {
		t.Fatalf("member: access %v create %v", m.MayAccess(), m.MayCreate())
	}
	if len(m.GrantedIDs) != 2 || m.GrantedRoles[0] != "Manager" || m.GrantedRoles[1] != "NodeManager" {
		t.Fatalf("member granted %v %v", m.GrantedIDs, m.GrantedRoles)
	}
	// Teams named by several rules count once among the managed teams.
	if len(m.ManagedIDs) != 3 {
		t.Fatalf("managed %v", m.ManagedIDs)
	}

	s := EvaluateClaimRules(rules, stranger)
	if s.MayAccess() || s.MayCreate() || len(s.GrantedIDs) != 0 {
		t.Fatalf("stranger: access %v create %v granted %v", s.MayAccess(), s.MayCreate(), s.GrantedIDs)
	}
	// The teams it does not get are still managed: a stranger who had them by
	// hand loses them at the next sign-in, were it allowed.
	if len(s.ManagedIDs) != 3 {
		t.Fatalf("stranger managed %v", s.ManagedIDs)
	}
}

func TestSessionTokens(t *testing.T) {
	p := newFakeProvider(t)
	key := bytes.Repeat([]byte{7}, 32)
	o, mr := newTestOIDC(t, p, func(c *OIDCConfig) { c.TokenKey = key })
	ctx := context.Background()

	signIn := func(o *OIDC) (string, *Session) {
		t.Helper()
		loginID, authURL, err := o.BeginLogin(ctx, "/")
		if err != nil {
			t.Fatal(err)
		}
		code, state := p.authorize(t, authURL)
		claims, _, idToken, err := o.FinishLogin(ctx, loginID, state, code)
		if err != nil {
			t.Fatal(err)
		}
		s := Session{UserID: 7, Email: claims.Email, Issuer: claims.Issuer, Subject: claims.Subject, IDToken: idToken}
		s.WithToken(claims.Token())
		id, err := o.CreateSession(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		stored, _ := o.readSession(ctx, hashKey("session", id))
		return id, stored
	}
	id, stored := signIn(o)
	// Only the ciphertext reaches the store.
	raw, _ := mr.Get(hashKey("session", id))
	if strings.Contains(raw, "opaque") || strings.Contains(raw, "refresh-0") || stored.Tokens == "" {
		t.Fatalf("tokens stored in clear, or not stored: %s", raw)
	}
	if at, err := o.AccessToken(ctx, id); err != nil || at != "opaque" {
		t.Fatalf("access token: %q %v", at, err)
	}

	// Near its expiry, the access token is refreshed, once for concurrent callers,
	// and the rotated refresh token is kept.
	expire := func(id string) {
		t.Helper()
		key := hashKey("session", id)
		s, _ := o.readSession(ctx, key)
		tok, err := o.openTokens(s)
		if err != nil {
			t.Fatal(err)
		}
		tok.Expiry = time.Now().Add(10 * time.Second)
		if s.Tokens, err = o.sealTokens(s, *tok); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(s)
		_ = mr.Set(key, string(b))
	}
	expire(id)
	var wg sync.WaitGroup
	got := make([]string, 5)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], _ = o.AccessToken(ctx, id)
		}(i)
	}
	wg.Wait()
	for _, at := range got {
		if at != "access-1" {
			t.Fatalf("concurrent refresh: %v", got)
		}
	}
	if p.refreshes != 1 {
		t.Fatalf("%d refreshes, want 1", p.refreshes)
	}
	s, _ := o.readSession(ctx, hashKey("session", id))
	if tok, _ := o.openTokens(s); tok == nil || tok.RefreshToken != "refresh-1" {
		t.Fatalf("rotated refresh token not kept: %+v", tok)
	}

	// The tokens of a session copied into another user's session do not open.
	other := *s
	other.Subject = "user-2"
	if _, err := o.openTokens(&other); !errors.Is(err, ErrNoToken) {
		t.Fatalf("tokens opened under another identity: %v", err)
	}

	// A refresh the provider refuses drops the tokens.
	p.mu.Lock()
	p.refreshTokens = map[string]bool{}
	p.mu.Unlock()
	expire(id)
	if _, err := o.AccessToken(ctx, id); !errors.Is(err, ErrNoToken) {
		t.Fatalf("refused refresh: %v", err)
	}
	if s, _ := o.readSession(ctx, hashKey("session", id)); s == nil || s.Tokens != "" {
		t.Fatalf("tokens kept after a refused refresh, or session lost: %+v", s)
	}

	// Without a key, nothing is kept.
	plain, _ := newTestOIDC(t, p, nil)
	id2, stored2 := signIn(plain)
	if stored2.Tokens != "" {
		t.Fatal("tokens kept without a key")
	}
	if _, err := plain.AccessToken(ctx, id2); !errors.Is(err, ErrNoToken) {
		t.Fatalf("no key: %v", err)
	}
}

// TestSignInExtensions checks what a sign-in tells of itself: its identity at the
// provider and its ends, never a token.
func TestSignInExtensions(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	sess := &Session{
		UserID: 1, Email: "user@example.com", Issuer: "https://idp.example.com", Subject: "sub-1",
		IDToken: "id-token", Tokens: "sealed-tokens",
		LastSeen: now, Expires: now.Add(90 * time.Minute),
	}
	ext := sessionExtensions(sess, time.Hour)
	if ext[XIssuer] != sess.Issuer || ext[XSubject] != sess.Subject {
		t.Fatalf("identity: %v", ext)
	}
	if ext[XExpires] != "2026-10-13T13:30:00Z" || ext[XIdleExpires] != "2026-10-13T13:00:00Z" {
		t.Fatalf("ends: %v", ext)
	}
	for k, v := range ext {
		if strings.Contains(v, "token") {
			t.Fatalf("%s carries a token: %q", k, v)
		}
	}
	// The idle end is within the absolute end.
	sess.LastSeen = now.Add(80 * time.Minute)
	if got := sessionExtensions(sess, time.Hour)[XIdleExpires]; got != "2026-10-13T13:30:00Z" {
		t.Fatalf("idle end beyond the absolute end: %s", got)
	}

	ext = bearerExtensions(&verifiedToken{issuer: "https://idp.example.com", subject: "sub-2", expires: now.Add(5 * time.Minute)})
	if ext[XSubject] != "sub-2" || ext[XExpires] != "2026-10-13T12:05:00Z" || ext[XIdleExpires] != "" {
		t.Fatalf("bearer: %v", ext)
	}
}
