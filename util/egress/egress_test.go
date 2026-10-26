package egress

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestPolicyCheck(t *testing.T) {
	allowed, err := ParsePrefixes([]string{"10.1.0.0/16", "127.0.0.1", "fd00::/8"})
	if err != nil {
		t.Fatal(err)
	}
	strict, open := Policy{}, Policy{Allowed: allowed}
	for _, c := range []struct {
		addr              string
		strictOK, allowOK bool
	}{
		{"93.184.216.34", true, true},
		{"2606:2800:220:1::1", true, true},
		{"169.254.169.254", false, false},
		{"::ffff:169.254.169.254", false, false},
		{"fe80::1", false, false},
		{"0.0.0.0", false, false},
		{"::", false, false},
		{"224.0.0.1", false, false},
		{"127.0.0.1", false, true},
		{"127.0.0.2", false, false},
		{"::ffff:127.0.0.1", false, true},
		{"::1", false, false},
		{"10.1.2.3", false, true},
		{"10.2.0.1", false, false},
		{"192.168.1.1", false, false},
		{"172.16.0.1", false, false},
		{"100.64.0.1", false, false},
		{"fd12::1", false, true},
		{"fc00::1", false, false},
		{"64:ff9b::7f00:1", false, false},
		{"2002:7f00:1::", false, false},
	} {
		addr := netip.MustParseAddr(c.addr)
		if err := strict.Check(addr); (err == nil) != c.strictOK {
			t.Errorf("strict %s: %v, want allowed=%v", c.addr, err, c.strictOK)
		}
		if err := open.Check(addr); (err == nil) != c.allowOK {
			t.Errorf("allowed %s: %v, want allowed=%v", c.addr, err, c.allowOK)
		}
	}
}

func TestParsePrefixesRefusesGarbage(t *testing.T) {
	if _, err := ParsePrefixes([]string{"10.0.0.0/8", "not-a-net"}); err == nil {
		t.Error("a garbage prefix was accepted")
	}
}

// TestClientRefusesLoopback checks the dialer refuses the resolved address: the
// test server listens on 127.0.0.1, reached by name or by address.
func TestClientRefusesLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	strict := NewClient(Options{Timeout: 5 * time.Second})
	for _, url := range []string{srv.URL, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)} {
		_, err := strict.Get(url)
		if err == nil || !errors.Is(err, ErrRefused) {
			t.Errorf("%s: err %v, want a refusal", url, err)
		}
	}
	loopback, _ := ParsePrefixes([]string{"127.0.0.1"})
	allowed := NewClient(Options{Policy: Policy{Allowed: loopback}, Timeout: 5 * time.Second})
	resp, err := allowed.Get(srv.URL)
	if err != nil {
		t.Fatalf("allowed loopback refused: %v", err)
	}
	_ = resp.Body.Close()
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer srv.Close()
	loopback, _ := ParsePrefixes([]string{"127.0.0.1"})
	client := NewClient(Options{Policy: Policy{Allowed: loopback}, Timeout: 5 * time.Second})
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Errorf("status %d, want the 302 itself", resp.StatusCode)
	}
}

func TestReadLimited(t *testing.T) {
	if b, err := ReadLimited(strings.NewReader("12345"), 5); err != nil || string(b) != "12345" {
		t.Errorf("at the limit: %q %v", b, err)
	}
	if _, err := ReadLimited(strings.NewReader("123456"), 5); !errors.Is(err, ErrTooLarge) {
		t.Errorf("over the limit: %v", err)
	}
}
