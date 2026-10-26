package serverhandlers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/safe"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/egress"
)

func testConnectorAPI(t *testing.T) *Api {
	t.Helper()
	key := base64.StdEncoding.EncodeToString(make([]byte, safe.KeySize))
	ring, err := safe.ParseKeyring([]byte("keys:\n- id: k1\n  key: " + key + "\n  active: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	return &Api{Safe: &safe.Service{Ring: ring}}
}

// testConnector returns a connector to base, its secrets sealed as the API
// seals them.
func testConnector(t *testing.T, a *Api, base, authType string, auth map[string]string, secrets map[string]string) *cdb.FormConnector {
	t.Helper()
	conn := &cdb.FormConnector{
		ID: 7, Name: "test", BaseURL: base, Methods: []string{"GET", "POST"}, Paths: []string{"/servers"},
		Networks: []string{"127.0.0.1"}, TimeoutS: 5, MaxBytes: 1 << 20, AuthType: authType, Auth: auth,
		TLS: map[string]string{}, Updated: time.Now(),
	}
	sealed, err := a.sealFormConnectorSecrets(conn.ID, secrets)
	if err != nil {
		t.Fatal(err)
	}
	conn.Secrets = sealed
	return conn
}

func TestConnectorDoAuthMethods(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"id":1}]}`))
	}))
	defer srv.Close()
	a := testConnectorAPI(t)
	for _, c := range []struct {
		name    string
		auth    string
		config  map[string]string
		secrets map[string]string
		check   func(*http.Request) bool
	}{
		{"none", "none", nil, nil, func(r *http.Request) bool { return r.Header.Get("Authorization") == "" }},
		{"basic", "basic", map[string]string{"username": "u"}, map[string]string{"password": "p"}, func(r *http.Request) bool {
			user, pass, ok := r.BasicAuth()
			return ok && user == "u" && pass == "p"
		}},
		{"bearer", "bearer", nil, map[string]string{"token": "t0k"}, func(r *http.Request) bool {
			return r.Header.Get("Authorization") == "Bearer t0k"
		}},
		{"api key header", "api_key", map[string]string{"in": "header", "name": "X-Api-Key"}, map[string]string{"api_key": "k3y"}, func(r *http.Request) bool {
			return r.Header.Get("X-Api-Key") == "k3y"
		}},
		{"api key query", "api_key", map[string]string{"in": "query", "name": "apikey"}, map[string]string{"api_key": "k3y"}, func(r *http.Request) bool {
			return r.URL.Query().Get("apikey") == "k3y" && r.URL.Query().Get("status") == "up"
		}},
	} {
		conn := testConnector(t, a, srv.URL+"/api", c.auth, c.config, c.secrets)
		resp, err := a.connectorDo(context.Background(), conn, connectorRequest{
			Method: "GET", Path: "/servers/a%2Fb", Query: map[string][]string{"status": {"up"}},
		})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !c.check(got) {
			t.Errorf("%s: the request does not carry the credentials: %v %v", c.name, got.Header, got.URL)
		}
		if got.URL.EscapedPath() != "/api/servers/a%2Fb" {
			t.Errorf("%s: path %s, want the escaped segment kept", c.name, got.URL.EscapedPath())
		}
		if strings.Contains(resp.URL, "k3y") {
			t.Errorf("%s: the url shown carries the api key: %s", c.name, resp.URL)
		}
		if rows, err := connectorRows(resp.Body, "items"); err != nil || len(rows) != 1 {
			t.Errorf("%s: rows %v %v", c.name, rows, err)
		}
	}
}

func TestConnectorDoRefusals(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	a := testConnectorAPI(t)
	conn := testConnector(t, a, srv.URL, "none", nil, nil)
	for name, r := range map[string]connectorRequest{
		"method":       {Method: "DELETE", Path: "/servers/1"},
		"path":         {Method: "GET", Path: "/users"},
		"prefix only":  {Method: "GET", Path: "/serversX"},
		"dot segment":  {Method: "GET", Path: "/servers/../users"},
		"encoded dots": {Method: "GET", Path: "/servers/%2e%2e/users"},
		"query":        {Method: "GET", Path: "/servers?x=1"},
	} {
		var refusal *connectorRefusal
		if _, err := a.connectorDo(context.Background(), conn, r); !asConnectorRefusal(err, &refusal) {
			t.Errorf("%s: err %v, want a refusal", name, err)
		}
	}
	// Without the loopback among its networks, the connector reaches nothing local.
	conn.Networks = nil
	if _, err := a.connectorDo(context.Background(), conn, connectorRequest{Method: "GET", Path: "/servers"}); !errors.Is(err, egress.ErrRefused) {
		t.Errorf("loopback: err %v, want egress.ErrRefused", err)
	}
}

func TestConnectorDoAnswerTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
	}))
	defer srv.Close()
	a := testConnectorAPI(t)
	conn := testConnector(t, a, srv.URL, "none", nil, nil)
	conn.MaxBytes = 1024
	if _, err := a.connectorDo(context.Background(), conn, connectorRequest{Method: "GET", Path: "/servers"}); !errors.Is(err, egress.ErrTooLarge) {
		t.Errorf("err %v, want egress.ErrTooLarge", err)
	}
}

// testPKI is a CA and the certificates it signs, for the TLS tests.
type testPKI struct {
	caPEM []byte
	ca    *x509.Certificate
	key   *ecdsa.PrivateKey
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	return &testPKI{caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca: ca, key: key}
}

// issue returns a certificate and its key, PEM, for a server (127.0.0.1) or a client.
func (p *testPKI) issue(t *testing.T, serial int64, server bool) ([]byte, []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func (p *testPKI) tlsServer(t *testing.T, h http.Handler, clientAuth tls.ClientAuthType) *httptest.Server {
	t.Helper()
	certPEM, keyPEM := p.issue(t, 2, true)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(p.ca)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: clientAuth, ClientCAs: pool}
	srv.StartTLS()
	return srv
}

func TestConnectorDoMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	srv := pki.tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"cn":"` + r.TLS.PeerCertificates[0].Subject.CommonName + `"}]`))
	}), tls.RequireAndVerifyClientCert)
	defer srv.Close()
	a := testConnectorAPI(t)
	clientCert, clientKey := pki.issue(t, 3, false)
	conn := testConnector(t, a, srv.URL, "none", nil, map[string]string{"client_key": string(clientKey)})
	conn.TLS = map[string]string{"ca_bundle": string(pki.caPEM), "client_cert": string(clientCert)}
	resp, err := a.connectorDo(context.Background(), conn, connectorRequest{Method: "GET", Path: "/servers"})
	if err != nil || resp.Status != 200 {
		t.Fatalf("mutual TLS: %v %+v", err, resp)
	}
	// Without the client certificate, the server refuses the handshake.
	conn.TLS = map[string]string{"ca_bundle": string(pki.caPEM)}
	if _, err := a.connectorDo(context.Background(), conn, connectorRequest{Method: "GET", Path: "/servers"}); err == nil {
		t.Error("a call without client certificate got through")
	}
	// Without the CA, the server certificate is not trusted.
	conn.TLS = map[string]string{}
	if _, err := a.connectorDo(context.Background(), conn, connectorRequest{Method: "GET", Path: "/servers"}); err == nil {
		t.Error("an untrusted server certificate was accepted")
	}
}

func TestConnectorDoOAuth2ClientCredentials(t *testing.T) {
	pki := newTestPKI(t)
	var tokens, revoked atomic.Int32
	srv := pki.tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			_ = r.ParseForm()
			id, secret, _ := r.BasicAuth()
			if id != "cid" || secret != "csecret" || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("audience") != "cmdb" {
				http.Error(w, "bad client", http.StatusUnauthorized)
				return
			}
			n := tokens.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok" + string(rune('0'+n)), "token_type": "Bearer", "expires_in": 3600})
		default:
			// The first token is revoked once: the call is retried with a new one.
			if r.Header.Get("Authorization") == "Bearer tok1" && revoked.Add(1) == 2 {
				http.Error(w, "revoked", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`[]`))
		}
	}), tls.NoClientCert)
	defer srv.Close()
	a := testConnectorAPI(t)
	conn := testConnector(t, a, srv.URL, "oauth2_client_credentials",
		map[string]string{"token_url": srv.URL + "/token", "client_id": "cid", "audience": "cmdb", "auth_style": "header"},
		map[string]string{"client_secret": "csecret"})
	conn.TLS = map[string]string{"ca_bundle": string(pki.caPEM)}
	for i := 0; i < 3; i++ {
		resp, err := a.connectorDo(context.Background(), conn, connectorRequest{Method: "GET", Path: "/servers"})
		if err != nil || resp.Status != 200 {
			t.Fatalf("call %d: %v %+v", i, err, resp)
		}
	}
	// One token for the first two calls, a second once the first was refused.
	if n := tokens.Load(); n != 2 {
		t.Errorf("%d tokens fetched, want 2", n)
	}
	forgetConnectorToken(conn.ID)
}

func TestBuildFormConnector(t *testing.T) {
	str := func(s string) *string { return &s }
	header, query := server.FormConnectorApiKeyInHeader, server.FormConnectorApiKeyInQuery
	valid := func() server.FormConnectorInput {
		return server.FormConnectorInput{Name: "cmdb", BaseUrl: "https://cmdb.example/api/", Auth: server.FormConnectorAuth{Type: server.FormConnectorAuthNone}}
	}
	conn, secrets, err := buildFormConnector(valid(), nil, nil)
	if err != nil || conn.BaseURL != "https://cmdb.example/api" || len(conn.Methods) != 1 || len(secrets) != 0 {
		t.Fatalf("minimal: %+v %v %v", conn, secrets, err)
	}
	for name, c := range map[string]struct {
		edit    func(*server.FormConnectorInput)
		current map[string]string
		changes map[string]*string
		want    string
	}{
		"bad name":         {func(in *server.FormConnectorInput) { in.Name = "a b" }, nil, nil, "name"},
		"relative url":     {func(in *server.FormConnectorInput) { in.BaseUrl = "/api" }, nil, nil, "base_url"},
		"url with creds":   {func(in *server.FormConnectorInput) { in.BaseUrl = "https://u:p@x.example" }, nil, nil, "base_url"},
		"url with query":   {func(in *server.FormConnectorInput) { in.BaseUrl = "https://x.example/?a=1" }, nil, nil, "base_url"},
		"ftp":              {func(in *server.FormConnectorInput) { in.BaseUrl = "ftp://x.example" }, nil, nil, "base_url"},
		"bad method":       {func(in *server.FormConnectorInput) { in.Methods = &[]server.FormConnectorMethod{"TRACE"} }, nil, nil, "methods"},
		"dot path":         {func(in *server.FormConnectorInput) { in.Paths = &[]string{"/a/../b"} }, nil, nil, "paths"},
		"relative path":    {func(in *server.FormConnectorInput) { in.Paths = &[]string{"servers"} }, nil, nil, "paths"},
		"bad network":      {func(in *server.FormConnectorInput) { in.Networks = &[]string{"lan"} }, nil, nil, "networks"},
		"auth header":      {func(in *server.FormConnectorInput) { in.Headers = &map[string]string{"Authorization": "x"} }, nil, nil, "headers"},
		"header injection": {func(in *server.FormConnectorInput) { in.Headers = &map[string]string{"X-A": "a\r\nX-B: b"} }, nil, nil, "headers"},
		"timeout":          {func(in *server.FormConnectorInput) { t := 0; in.Timeout = &t }, nil, nil, "timeout"},
		"basic no user":    {func(in *server.FormConnectorInput) { in.Auth.Type = server.FormConnectorAuthBasic }, nil, nil, "username"},
		"basic no pass": {func(in *server.FormConnectorInput) {
			in.Auth = server.FormConnectorAuth{Type: server.FormConnectorAuthBasic, Username: str("u")}
		}, nil, nil, "password"},
		"bearer removed": {func(in *server.FormConnectorInput) { in.Auth.Type = server.FormConnectorAuthBearer },
			map[string]string{"token": "t"}, map[string]*string{"token": nil}, "token"},
		"api key no in": {func(in *server.FormConnectorInput) {
			in.Auth = server.FormConnectorAuth{Type: server.FormConnectorAuthApiKey, Name: str("X-Key")}
		}, map[string]string{"api_key": "k"}, nil, "auth.in"},
		"api key as cookie": {func(in *server.FormConnectorInput) {
			in.Auth = server.FormConnectorAuth{Type: server.FormConnectorAuthApiKey, In: &header, Name: str("Cookie")}
		}, map[string]string{"api_key": "k"}, nil, "auth.name"},
		"oauth2 http token url": {func(in *server.FormConnectorInput) {
			in.Auth = server.FormConnectorAuth{Type: server.FormConnectorAuthOAuth2ClientCredentials, TokenUrl: str("http://idp.example/token"), ClientId: str("c")}
		}, map[string]string{"client_secret": "s"}, nil, "token_url"},
		"credentials over http": {func(in *server.FormConnectorInput) {
			in.BaseUrl = "http://x.example"
			in.Auth = server.FormConnectorAuth{Type: server.FormConnectorAuthBearer}
		}, map[string]string{"token": "t"}, nil, "https"},
		"client cert without key": {func(in *server.FormConnectorInput) {
			in.Tls = &server.FormConnectorTLS{ClientCert: str("-----BEGIN CERTIFICATE-----")}
		}, nil, nil, "client_key"},
		"bad ca": {func(in *server.FormConnectorInput) { in.Tls = &server.FormConnectorTLS{CaBundle: str("garbage")} }, nil, nil, "ca_bundle"},
	} {
		in := valid()
		c.edit(&in)
		_, _, err := buildFormConnector(in, c.current, c.changes)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want one about %q", name, err, c.want)
		}
	}
	// A secret the authentication does not use is not kept.
	in := valid()
	in.Auth = server.FormConnectorAuth{Type: server.FormConnectorAuthApiKey, In: &query, Name: str("apikey")}
	_, secrets, err = buildFormConnector(in, map[string]string{"api_key": "k", "password": "old"}, nil)
	if err != nil || len(secrets) != 1 || secrets["api_key"] != "k" {
		t.Errorf("kept secrets %v %v, want the api key alone", secrets, err)
	}
}

func TestConnectorRows(t *testing.T) {
	for _, c := range []struct {
		body any
		path string
		n    int
		ok   bool
	}{
		{[]any{1, 2}, "", 2, true},
		{map[string]any{"data": []any{1}}, "", 1, true},
		{map[string]any{"items": []any{1, 2, 3}}, "items", 3, true},
		{map[string]any{"a": map[string]any{"b": []any{1}}}, "a.b", 1, true},
		{map[string]any{"items": []any{1}}, "", 0, false},
		{map[string]any{"items": "x"}, "items", 0, false},
		{map[string]any{}, "missing", 0, false},
	} {
		rows, err := connectorRows(c.body, c.path)
		if (err == nil) != c.ok || len(rows) != c.n {
			t.Errorf("%v at %q: %v %v", c.body, c.path, rows, err)
		}
	}
}
