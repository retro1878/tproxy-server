package session

import (
	"errors"
	"net"
	"strings"
	"syscall"

	"github.com/telegramdesktop/tproxy-server/internal/config"
	"github.com/telegramdesktop/tproxy-server/internal/frame"
)

const maxTunnelHostLength = 253

var (
	errTunnelDestination = errors.New("tunnel destination is malformed")
	errTunnelDenied      = errors.New("tunnel destination is not permitted")
)

// allowsOpenPayload reports whether this session's profile takes a destination
// named by the client in OPEN. Only a tunnel profile does.
func (s *Session) allowsOpenPayload() bool {
	return s.profile.Kind == config.KindTunnel
}

// parseTunnelDestination reads the destination a tunnel client named in its
// OPEN payload. It checks the shape only; the admission policy runs against the
// resolved address in Dialer.Control, so a name that resolves to a refused
// address is caught without a second resolution or a rebinding window between
// the check and the dial.
func parseTunnelDestination(payload []byte) (string, error) {
	if len(payload) == 0 || len(payload) > frame.MaxOpenPayload {
		return "", errTunnelDestination
	}
	// SplitHostPort accepts a bracketed IPv6 literal and rejects an unbracketed
	// colon mess. DNS names are case-insensitive, so canonicalise to lowercase.
	host, port, err := net.SplitHostPort(strings.ToLower(string(payload)))
	if err != nil {
		return "", errTunnelDestination
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > maxTunnelHostLength {
		return "", errTunnelDestination
	}
	if net.ParseIP(host) == nil && !validTunnelHostname(host) {
		return "", errTunnelDestination
	}
	if !validDecimalPort(port) {
		return "", errTunnelDestination
	}
	return net.JoinHostPort(host, port), nil
}

func validTunnelHostname(host string) bool {
	if host == "" || len(host) > maxTunnelHostLength {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			character := label[i]
			if (character >= 'a' && character <= 'z') ||
				(character >= '0' && character <= '9') ||
				character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func validDecimalPort(port string) bool {
	if port == "" || len(port) > 5 {
		return false
	}
	value := 0
	for i := 0; i < len(port); i++ {
		if port[i] < '0' || port[i] > '9' {
			return false
		}
		value = value*10 + int(port[i]-'0')
	}
	return value >= 1 && value <= 65535
}

// tunnelDialControl returns the admission policy for this session's profile, or
// nil when every resolved address is allowed. It is installed as
// Dialer.Control, which runs after resolution and before connect, so the
// address it sees is exactly the one that would be dialed.
func (s *Session) tunnelDialControl() func(string, string, syscall.RawConn) error {
	if s.profile.Kind != config.KindTunnel || s.profile.AllowPrivateTargets {
		return nil
	}
	return denyPrivateTunnelAddress
}

// denyPrivateTunnelAddress stops a client from using the relay as a way into
// its own host or private network: the MTProxy backend and admin listener,
// link-local metadata services, and anything else unroutable.
func denyPrivateTunnelAddress(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errTunnelDenied
	}
	ip := net.ParseIP(host)
	if ip == nil || isDeniedTunnelAddress(ip) {
		return errTunnelDenied
	}
	return nil
}

func isDeniedTunnelAddress(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast()
}
