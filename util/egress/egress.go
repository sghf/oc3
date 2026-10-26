// Package egress makes the HTTP calls oc3 sends to hosts its users name: the
// external APIs of the forms. A user who may write a form must not get, through
// oc3, at what only oc3 reaches: its own listeners, the cloud metadata service,
// the hosts of its private network. The address checked is the one the client
// connects to, once the name is resolved, so that a name resolving to a refused
// address, or changing its answer between two lookups, is refused all the same.
package egress

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

// ErrRefused tells an address the policy refuses.
var ErrRefused = errors.New("egress: destination refused")

// ErrTooLarge tells an answer larger than the size allowed.
var ErrTooLarge = errors.New("egress: the answer is larger than allowed")

// never are the addresses no policy allows: link-local, the cloud metadata
// service among them, multicast and the unspecified address.
var never = prefixes(
	"0.0.0.0/8",      // "this network", the unspecified address
	"169.254.0.0/16", // link-local, cloud metadata 169.254.169.254
	"224.0.0.0/4",    // multicast
	"255.255.255.255/32",
	"::/128",
	"fe80::/10", // link-local
	"ff00::/8",  // multicast
)

// restricted are the addresses allowed only when a prefix of the policy covers
// them: loopback, private and shared networks, the ranges reserved for
// documentation, benchmarks or future use, and the IPv6 ranges that carry an
// IPv4 address (NAT64, 6to4), which could carry a restricted one.
var restricted = prefixes(
	"127.0.0.0/8",
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"100.64.0.0/10", // carrier-grade NAT
	"192.0.0.0/24",
	"192.0.2.0/24",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"240.0.0.0/4",
	"::1/128",
	"fc00::/7", // unique local
	"64:ff9b::/96",
	"64:ff9b:1::/48",
	"2001:db8::/32",
	"2002::/16",
)

func prefixes(list ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(list))
	for i, s := range list {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// Policy tells the addresses a client may connect to: the public ones, and the
// restricted ones a prefix of Allowed covers. Link-local, multicast and the
// unspecified addresses are never allowed.
type Policy struct {
	Allowed []netip.Prefix
}

// ParsePrefixes reads CIDR prefixes, "10.1.0.0/16" or "fd00::/8"; a bare
// address is a prefix of its own.
func ParsePrefixes(list []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(list))
	for _, s := range list {
		if p, err := netip.ParsePrefix(s); err == nil {
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("%q is neither an address nor a CIDR prefix", s)
		}
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// Check refuses an address the policy does not allow.
func (p Policy) Check(addr netip.Addr) error {
	addr = addr.Unmap()
	for _, n := range never {
		if n.Contains(addr) {
			return fmt.Errorf("%w: %s is a link-local, multicast or unspecified address", ErrRefused, addr)
		}
	}
	for _, n := range restricted {
		if !n.Contains(addr) {
			continue
		}
		for _, a := range p.Allowed {
			if a.Contains(addr) {
				return nil
			}
		}
		return fmt.Errorf("%w: %s is a loopback, private or reserved address none of the allowed networks covers", ErrRefused, addr)
	}
	return nil
}

// control checks the address a dialer is about to connect to.
func (p Policy) control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: unparsable address %q", ErrRefused, address)
	}
	return p.Check(ap.Addr())
}

// Options of a client.
type Options struct {
	Policy Policy
	// Timeout bounds a whole call, the answer read included.
	Timeout time.Duration
	// TLS is the client TLS configuration, its CA and client certificate say;
	// nil for the defaults.
	TLS *tls.Config
}

// NewClient returns a client that connects only where the policy allows, never
// follows a redirect (a redirect would lead elsewhere than the URL checked, its
// answer is returned as is), and ignores the proxy settings of the environment:
// through a proxy, the address checked would be the proxy's.
func NewClient(o Options) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second, Control: o.Policy.control}
	tlsConfig := o.TLS
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		TLSClientConfig:       tlsConfig,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: o.Timeout,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   o.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// ReadLimited reads r up to max bytes: ErrTooLarge beyond.
func ReadLimited(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("%w (%d bytes)", ErrTooLarge, max)
	}
	return b, nil
}
