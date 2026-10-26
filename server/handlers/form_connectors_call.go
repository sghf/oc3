package serverhandlers

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	neturl "net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/egress"
)

// connectorRequest is a call through a connector: its method, its path relative
// to the base url, escaped (formRestArgs), its query, and its JSON body.
type connectorRequest struct {
	Method string
	Path   string
	Query  neturl.Values
	Body   any
}

// connectorResponse is the answer of the external API: Body is the decoded
// JSON, nil when the answer is not JSON. URL is the url called, without the
// query parameter carrying an api key.
type connectorResponse struct {
	URL         string
	Status      int
	ContentType string
	Raw         []byte
	Body        any
}

// connectorRefusal is a call the connector does not allow: a method or a path
// it does not list. Nothing was sent.
type connectorRefusal struct{ msg string }

func (e *connectorRefusal) Error() string { return e.msg }

func asConnectorRefusal(err error, target **connectorRefusal) bool {
	return err != nil && errors.As(err, target)
}

// connectorPathAllowed tells whether the escaped path is under one of the
// prefixes, segment by segment: "/servers" allows "/servers" and "/servers/12",
// not "/serversX". No prefix allows every path.
func connectorPathAllowed(prefixes []string, path string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// connectorTokens caches the OAuth2 token sources by connector, renewed when the
// connector changes (its update date) or the API refuses a token.
var connectorTokens = struct {
	sync.Mutex
	m map[int64]connectorToken
}{m: map[int64]connectorToken{}}

type connectorToken struct {
	updated time.Time
	source  oauth2.TokenSource
}

func connectorTokenSource(conn *cdb.FormConnector, secrets map[string]string, client *http.Client) oauth2.TokenSource {
	connectorTokens.Lock()
	defer connectorTokens.Unlock()
	if cached, ok := connectorTokens.m[conn.ID]; ok && cached.updated.Equal(conn.Updated) {
		return cached.source
	}
	cfg := clientcredentials.Config{
		ClientID:     conn.Auth["client_id"],
		ClientSecret: secrets["client_secret"],
		TokenURL:     conn.Auth["token_url"],
		AuthStyle:    oauth2.AuthStyleInHeader,
	}
	if conn.Auth["auth_style"] == string(server.FormConnectorAuthStyleParams) {
		cfg.AuthStyle = oauth2.AuthStyleInParams
	}
	if scopes := strings.Fields(conn.Auth["scopes"]); len(scopes) > 0 {
		cfg.Scopes = scopes
	}
	if aud := conn.Auth["audience"]; aud != "" {
		cfg.EndpointParams = neturl.Values{"audience": {aud}}
	}
	// The token endpoint is reached through the same guarded client, with the
	// connector's timeout; the source outlives the request that made it.
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	source := oauth2.ReuseTokenSource(nil, cfg.TokenSource(ctx))
	connectorTokens.m[conn.ID] = connectorToken{updated: conn.Updated, source: source}
	return source
}

func forgetConnectorToken(id int64) {
	connectorTokens.Lock()
	delete(connectorTokens.m, id)
	connectorTokens.Unlock()
}

// connectorClient is the guarded client of a connector: its networks, its
// timeout, its TLS settings.
func connectorClient(conn *cdb.FormConnector, secrets map[string]string) (*http.Client, error) {
	networks, err := egress.ParsePrefixes(conn.Networks)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: conn.TLS["server_name"]}
	if ca := conn.TLS["ca_bundle"]; ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, errors.New("tls.ca_bundle holds no certificate")
		}
		tlsConfig.RootCAs = pool
	}
	if cert := conn.TLS["client_cert"]; cert != "" {
		pair, err := tls.X509KeyPair([]byte(cert), []byte(secrets["client_key"]))
		if err != nil {
			return nil, fmt.Errorf("the client certificate and its key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	return egress.NewClient(egress.Options{
		Policy:  egress.Policy{Allowed: networks},
		Timeout: time.Duration(conn.TimeoutS) * time.Second,
		TLS:     tlsConfig,
	}), nil
}

// connectorDo calls the external API of a connector. A method or a path the
// connector does not allow is a connectorRefusal, nothing sent. The answer is
// returned whatever its status; an error tells no answer came, or one too large.
func (a *Api) connectorDo(ctx context.Context, conn *cdb.FormConnector, r connectorRequest) (*connectorResponse, error) {
	if !slices.Contains(conn.Methods, r.Method) {
		return nil, &connectorRefusal{fmt.Sprintf("connector %s does not allow the method %s", conn.Name, r.Method)}
	}
	if r.Path == "" {
		r.Path = "/"
	}
	if r.Path != "/" && !isConnectorPath(r.Path) {
		return nil, &connectorRefusal{fmt.Sprintf("connector %s: %q is no path it may call", conn.Name, r.Path)}
	}
	if !connectorPathAllowed(conn.Paths, strings.TrimRight(r.Path, "/")) {
		return nil, &connectorRefusal{fmt.Sprintf("connector %s does not allow the path %s", conn.Name, r.Path)}
	}
	secrets, err := a.openFormConnectorSecrets(conn)
	if err != nil {
		return nil, err
	}
	client, err := connectorClient(conn, secrets)
	if err != nil {
		return nil, err
	}
	target, err := neturl.Parse(conn.BaseURL)
	if err != nil {
		return nil, err
	}
	// The path is escaped already: set as the raw path, kept as it is.
	if r.Path != "/" {
		target.RawPath = target.EscapedPath() + r.Path
		if target.Path, err = neturl.PathUnescape(target.RawPath); err != nil {
			return nil, &connectorRefusal{fmt.Sprintf("connector %s: unparsable path %s", conn.Name, r.Path)}
		}
	}
	query := neturl.Values{}
	for k, v := range r.Query {
		query[k] = v
	}
	shown := *target
	shown.RawQuery = query.Encode()
	if conn.AuthType == string(server.FormConnectorAuthApiKey) && conn.Auth["in"] == string(server.FormConnectorApiKeyInQuery) {
		query.Set(conn.Auth["name"], secrets["api_key"])
	}
	target.RawQuery = query.Encode()

	var payload []byte
	if r.Body != nil && r.Method != http.MethodGet {
		if payload, err = json.Marshal(r.Body); err != nil {
			return nil, err
		}
	}
	send := func() (*http.Response, error) {
		var body io.Reader
		if payload != nil {
			body = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, r.Method, target.String(), body)
		if err != nil {
			return nil, err
		}
		for k, v := range conn.Headers {
			req.Header.Set(k, v)
		}
		req.Header.Set("Accept", "application/json")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		switch conn.AuthType {
		case string(server.FormConnectorAuthBasic):
			req.SetBasicAuth(conn.Auth["username"], secrets["password"])
		case string(server.FormConnectorAuthBearer):
			req.Header.Set("Authorization", "Bearer "+secrets["token"])
		case string(server.FormConnectorAuthApiKey):
			if conn.Auth["in"] == string(server.FormConnectorApiKeyInHeader) {
				req.Header.Set(conn.Auth["name"], secrets["api_key"])
			}
		case string(server.FormConnectorAuthOAuth2ClientCredentials):
			token, err := connectorTokenSource(conn, secrets, client).Token()
			if err != nil {
				return nil, fmt.Errorf("cannot get an access token from %s: %w", conn.Auth["token_url"], err)
			}
			token.SetAuthHeader(req)
		}
		return client.Do(req)
	}
	started := time.Now()
	resp, err := send()
	// A token refused before its end of life, revoked say: a new one, once.
	if err == nil && resp.StatusCode == http.StatusUnauthorized && conn.AuthType == string(server.FormConnectorAuthOAuth2ClientCredentials) {
		_ = resp.Body.Close()
		forgetConnectorToken(conn.ID)
		resp, err = send()
	}
	out := &connectorResponse{URL: shown.String()}
	if err != nil {
		slog.Info("form connector call failed", "connector", conn.Name, "method", r.Method, "url", out.URL, "error", err)
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()
	out.Status = resp.StatusCode
	out.ContentType = resp.Header.Get("Content-Type")
	if out.Raw, err = egress.ReadLimited(resp.Body, conn.MaxBytes); err != nil {
		return out, err
	}
	if mediaType, _, _ := mime.ParseMediaType(out.ContentType); mediaType == "application/json" || strings.HasSuffix(mediaType, "+json") || out.ContentType == "" {
		decoder := json.NewDecoder(bytes.NewReader(out.Raw))
		decoder.UseNumber()
		if decoder.Decode(&out.Body) != nil {
			out.Body = nil
		}
	}
	slog.Info("form connector call", "connector", conn.Name, "method", r.Method, "url", out.URL,
		"status", out.Status, "duration_ms", time.Since(started).Milliseconds(), "bytes", len(out.Raw))
	return out, nil
}

// connectorErrorText is what the browser is told of a failed answer: its
// status and the start of its text, the external API's own words being of use
// to the form's author, but not at any length.
func connectorErrorText(conn *cdb.FormConnector, resp *connectorResponse) string {
	text := strings.TrimSpace(string(resp.Raw))
	if m, ok := resp.Body.(map[string]any); ok {
		for _, k := range []string{"message", "error", "detail", "text", "title"} {
			if v, ok := m[k].(string); ok && v != "" {
				text = v
				break
			}
		}
	}
	if len(text) > 300 {
		text = text[:300] + "…"
	}
	return fmt.Sprintf("connector %s answered %d: %s", conn.Name, resp.Status, text)
}

// connectorRows reads the list of an answer: at resultPath ("items",
// "data.results") when given; else the answer itself when a list, else its data.
func connectorRows(body any, resultPath string) ([]any, error) {
	cur := body
	if resultPath != "" {
		for _, key := range strings.Split(resultPath, ".") {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("the answer has no %s", resultPath)
			}
			if cur, ok = m[key]; !ok {
				return nil, fmt.Errorf("the answer has no %s", resultPath)
			}
		}
	} else if m, ok := cur.(map[string]any); ok {
		cur = m["data"]
	}
	rows, ok := cur.([]any)
	if !ok {
		if resultPath == "" {
			return nil, errors.New("the answer is no list: set the ResultPath of the input")
		}
		return nil, fmt.Errorf("the answer's %s is no list", resultPath)
	}
	return rows, nil
}
