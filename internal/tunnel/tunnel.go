// Package tunnel defines the SSH tunnel interface and management.
package tunnel

import "context"

// Tunnel represents an SSH tunnel that forwards a local port to a remote address.
type Tunnel interface {
	// Open establishes the SSH tunnel and returns the local address the
	// database driver should connect to (e.g. "127.0.0.1:54321").
	Open(ctx context.Context) (localAddr string, err error)

	// Close tears down the tunnel and releases all resources.
	Close() error

	// LocalAddr returns the local address after Open() has been called.
	// Returns empty string if not yet opened.
	LocalAddr() string

	// IsOpen reports whether the tunnel is currently active.
	IsOpen() bool
}

// Config holds SSH tunnel configuration.
type Config struct {
	// Enabled controls whether the tunnel is used.
	Enabled bool

	// Host is the SSH bastion/jump host address.
	Host string

	// Port is the SSH port (default 22).
	Port int

	// Username for SSH authentication.
	Username string

	// Password for SSH password authentication (optional).
	Password string

	// KeyFile is the path to an SSH private key file.
	KeyFile string

	// RemoteHost is the database host to forward to (inferred from DB config).
	RemoteHost string

	// RemotePort is the database port to forward to (inferred from DB config).
	RemotePort int

	// KnownHostsFile is the path to an OpenSSH known_hosts file for host key
	// verification. If empty, defaults to ~/.ssh/known_hosts. Set Insecure to
	// true to skip verification (not recommended).
	KnownHostsFile string

	// Insecure disables host key verification when set to true. This makes the
	// tunnel vulnerable to man-in-the-middle attacks and should only be used in
	// development or testing.
	Insecure bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Port: 22,
	}
}

// NoopTunnel is a no-op implementation used when SSH is disabled.
type NoopTunnel struct{}

func (NoopTunnel) Open(_ context.Context) (string, error) { return "", nil }
func (NoopTunnel) Close() error                           { return nil }
func (NoopTunnel) LocalAddr() string                      { return "" }
func (NoopTunnel) IsOpen() bool                           { return false }
