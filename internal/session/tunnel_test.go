package session

import (
	"net"
	"testing"
	"time"

	"github.com/telegramdesktop/tproxy-server/internal/config"
	"github.com/telegramdesktop/tproxy-server/internal/frame"
)

// newTunnelSession builds an authenticated session on a tunnel profile. The
// profile has no backend, so every stream dials the destination its OPEN names.
func newTunnelSession(t *testing.T, allowPrivate bool) (*Manager, CreateResult) {
	t.Helper()
	configuration := testConfig("127.0.0.1:1")
	configuration.Profiles[0].Kind = config.KindTunnel
	configuration.Profiles[0].Backend = ""
	configuration.Profiles[0].AllowPrivateTargets = allowPrivate
	manager := NewManager(configuration, [32]byte{1})
	t.Cleanup(manager.Shutdown)
	bootstrap, err := manager.IssueBootstrap(&configuration.Profiles[0], "198.51.100.15")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(bootstrap, "198.51.100.15", frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	return manager, created
}

func TestParseTunnelDestination(t *testing.T) {
	valid := []struct{ input, want string }{
		{"example.com:443", "example.com:443"},
		{"EXAMPLE.com:443", "example.com:443"},
		{"example.com.:443", "example.com:443"},
		{"1.2.3.4:65535", "1.2.3.4:65535"},
		{"[::1]:443", "[::1]:443"},
		{"xn--e1afmkfd.xn--p1ai:80", "xn--e1afmkfd.xn--p1ai:80"},
	}
	for _, value := range valid {
		got, err := parseTunnelDestination([]byte(value.input))
		if err != nil || got != value.want {
			t.Fatalf("parseTunnelDestination(%q) = %q, %v; want %q", value.input, got, err, value.want)
		}
	}
	invalid := []string{
		"", "example.com", ":443", "example.com:", "example.com:0",
		"example.com:65536", "example.com:443a", "example.com: 443",
		"a b.com:443", "exa_mple.com:443", "-example.com:443",
		"example..com:443", "example.com:443:1", "::1:443",
	}
	for _, value := range invalid {
		if _, err := parseTunnelDestination([]byte(value)); err == nil {
			t.Fatalf("parseTunnelDestination(%q) was accepted", value)
		}
	}
	oversized := make([]byte, frame.MaxOpenPayload+1)
	for i := range oversized {
		oversized[i] = 'a'
	}
	if _, err := parseTunnelDestination(oversized); err == nil {
		t.Fatal("an oversized OPEN payload was accepted")
	}
}

func TestTunnelAddressPolicy(t *testing.T) {
	denied := []string{
		"127.0.0.1", "127.1.2.3", "10.0.0.1", "172.16.5.4", "192.168.1.1",
		"169.254.169.254", "0.0.0.0", "224.0.0.1", "::1", "fc00::1",
		"fe80::1", "ff02::1",
	}
	for _, value := range denied {
		if !isDeniedTunnelAddress(net.ParseIP(value)) {
			t.Fatalf("address %s was allowed", value)
		}
	}
	allowed := []string{"1.1.1.1", "8.8.8.8", "185.4.114.43", "2606:4700:4700::1111"}
	for _, value := range allowed {
		if isDeniedTunnelAddress(net.ParseIP(value)) {
			t.Fatalf("address %s was denied", value)
		}
	}
}

func TestTunnelOpenDialsTheDestinationTheClientNamed(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan string, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		buffer := make([]byte, 32)
		read, _ := connection.Read(buffer)
		received <- string(buffer[:read])
	}()

	// The test listener is on loopback, which the default policy refuses.
	_, created := newTunnelSession(t, true)
	destination := listener.Addr().String()
	if _, err := created.Session.ProcessUp(1, frame.Encode(frame.Open, 21, []byte(destination))); err != nil {
		t.Fatal(err)
	}
	if _, err := created.Session.ProcessUp(2, frame.Encode(frame.Data, 21, []byte("ping"))); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-received:
		if value != "ping" {
			t.Fatalf("the destination received %q, want %q", value, "ping")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the destination named in OPEN was not dialed and written to")
	}
}

func TestTunnelRefusesDeniedAndMalformedDestinations(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	manager, created := newTunnelSession(t, false)
	refusals := []struct {
		sequence uint64
		id       uint32
		payload  string
	}{
		// Well formed, but the policy refuses the address it resolves to.
		{1, 21, listener.Addr().String()},
		// Not a host:port at all, so it never reaches the dialer.
		{2, 22, "example.com"},
	}
	for _, refusal := range refusals {
		if _, err := created.Session.ProcessUp(refusal.sequence, frame.Encode(frame.Open, refusal.id, []byte(refusal.payload))); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool {
			created.Session.mu.Lock()
			defer created.Session.mu.Unlock()
			_, closed := created.Session.closedStreams[refusal.id]
			return closed
		})
	}
	waitFor(t, func() bool {
		created.Session.mu.Lock()
		defer created.Session.mu.Unlock()
		return len(created.Session.streams) == 0
	})
	// A refused destination closes only its own stream.
	if _, err := manager.Get(created.Token); err != nil {
		t.Fatalf("a refused destination closed the parent session: %v", err)
	}
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if connection, acceptErr := listener.Accept(); acceptErr == nil {
		_ = connection.Close()
		t.Fatal("a refused destination was dialed")
	}
}
