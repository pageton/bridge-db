package tunnel

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/pageton/bridge-db/internal/logger"
)

// SSHTunnel implements the Tunnel interface using SSH local port forwarding.
// It connects to a bastion host and forwards a random local port to a remote
// database address through the SSH connection.
type SSHTunnel struct {
	config Config

	mu        sync.Mutex
	client    *ssh.Client
	listener  net.Listener
	localAddr string
	open      bool
	done      chan struct{}
}

// NewSSHTunnel creates a new SSH tunnel from the given configuration.
func NewSSHTunnel(cfg Config) *SSHTunnel {
	return &SSHTunnel{
		config: cfg,
		done:   make(chan struct{}),
	}
}

// Open establishes the SSH connection and starts local port forwarding.
// It returns the local address (e.g. "127.0.0.1:54321") that the database
// driver should connect to.
func (t *SSHTunnel) Open(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.open {
		return t.localAddr, nil
	}

	log := logger.L().With("component", "tunnel", "ssh_host", t.config.Host)

	// 1. Build SSH client config with auth
	sshConfig, err := t.buildSSHConfig()
	if err != nil {
		return "", fmt.Errorf("ssh auth config: %w", err)
	}

	// 2. Dial the SSH server
	addr := fmt.Sprintf("%s:%d", t.config.Host, t.config.Port)
	log.Debug("connecting to SSH server", "address", addr)

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", fmt.Errorf("ssh dial %s: %w", addr, err)
	}

	// Wrap the net.Conn in an SSH connection
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, sshConfig)
	if err != nil {
		_ = conn.Close()
		return "", fmt.Errorf("ssh handshake %s: %w", addr, err)
	}
	t.client = ssh.NewClient(sshConn, chans, reqs)

	// 3. Set keepalive
	go t.keepalive()

	// 4. Listen on random local port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = t.client.Close()
		t.client = nil
		return "", fmt.Errorf("listen on local port: %w", err)
	}
	t.listener = listener
	t.localAddr = listener.Addr().String()

	// 5. Start forwarding goroutine
	go t.forward()

	t.open = true
	log.Debug("SSH tunnel established",
		"local_addr", t.localAddr,
		"remote_addr", fmt.Sprintf("%s:%d", t.config.RemoteHost, t.config.RemotePort),
	)

	return t.localAddr, nil
}

// Close shuts down the listener and SSH client.
func (t *SSHTunnel) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.open {
		return nil
	}

	// Signal the forwarding and keepalive goroutines to stop.
	close(t.done)
	t.done = make(chan struct{}) // reset for potential reuse

	var errs []error

	if t.listener != nil {
		if err := t.listener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close listener: %w", err))
		}
		t.listener = nil
	}

	if t.client != nil {
		if err := t.client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close ssh client: %w", err))
		}
		t.client = nil
	}

	t.localAddr = ""
	t.open = false

	if len(errs) > 0 {
		return fmt.Errorf("close tunnel: %v", errs)
	}
	return nil
}

// LocalAddr returns the local address after Open() has been called.
func (t *SSHTunnel) LocalAddr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.localAddr
}

// IsOpen reports whether the tunnel is currently active.
func (t *SSHTunnel) IsOpen() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.open
}

// ---------------------------------------------------------------------------
// Auth resolution
// ---------------------------------------------------------------------------

// buildSSHConfig creates an *ssh.ClientConfig with the appropriate auth method.
// Resolution order: key file → password → SSH agent.
func (t *SSHTunnel) buildSSHConfig() (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	if t.config.KeyFile != "" {
		auth, err := publicKeyAuth(t.config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("key file %s: %w", t.config.KeyFile, err)
		}
		authMethods = append(authMethods, auth)
	}

	if t.config.Password != "" {
		authMethods = append(authMethods, ssh.Password(t.config.Password))
	}

	if len(authMethods) == 0 {
		auth, err := agentAuth()
		if err != nil {
			return nil, fmt.Errorf("no auth method available (tried key file, password, ssh agent): %w", err)
		}
		authMethods = append(authMethods, auth)
	}

	if t.config.Username == "" {
		return nil, fmt.Errorf("ssh username is required")
	}

	var hostKeyCallback ssh.HostKeyCallback
	if t.config.Insecure {
		if os.Getenv("BRIDGE_ALLOW_INSECURE_SSH") != "1" {
			return nil, fmt.Errorf("ssh insecure mode requires BRIDGE_ALLOW_INSECURE_SSH=1 environment variable to be set")
		}
		logger.L().Warn("ssh tunnel: host key verification disabled (insecure mode)")
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	} else {
		cb, err := resolveHostKeyCallback(t.config.KnownHostsFile)
		if err != nil {
			return nil, fmt.Errorf("host key verification: %w (set insecure: true to skip)", err)
		}
		hostKeyCallback = cb
	}

	return &ssh.ClientConfig{
		User:            t.config.Username,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         30 * time.Second,
	}, nil
}

func resolveHostKeyCallback(knownHostsFile string) (ssh.HostKeyCallback, error) {
	if knownHostsFile != "" {
		return knownhosts.New(knownHostsFile)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory: %w", err)
	}

	defaultPath := filepath.Join(home, ".ssh", "known_hosts")
	if _, err := os.Stat(defaultPath); err == nil {
		return knownhosts.New(defaultPath)
	}

	return nil, fmt.Errorf("no known_hosts file found (checked %s)", defaultPath)
}

// publicKeyAuth reads a private key file and returns an ssh.AuthMethod.
func publicKeyAuth(keyFile string) (ssh.AuthMethod, error) {
	if len(keyFile) > 0 && keyFile[0] == '~' {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("expand home dir: %w", err)
		}
		keyFile = filepath.Clean(home + keyFile[1:])
	}

	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Try parsing without passphrase first
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		// If the key is encrypted, try with SSH agent instead of prompting
		if _, ok := err.(*ssh.PassphraseMissingError); ok {
			return nil, fmt.Errorf("key is passphrase-protected; use SSH agent (SSH_AUTH_SOCK) instead")
		}
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	return ssh.PublicKeys(signer), nil
}

// agentAuth attempts to authenticate using the SSH agent at SSH_AUTH_SOCK.
func agentAuth() (ssh.AuthMethod, error) {
	sock := os.Getenv("SSH_AUTH_SOCK")
	if sock == "" {
		return nil, fmt.Errorf("SSH_AUTH_SOCK not set")
	}

	conn, err := net.Dial("unix", sock)
	if err != nil {
		return nil, fmt.Errorf("connect to ssh agent: %w", err)
	}

	ag := agent.NewClient(conn)
	signers, err := ag.Signers()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("get signers from agent: %w", err)
	}

	if len(signers) == 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("ssh agent has no identities loaded")
	}

	return ssh.PublicKeys(signers...), nil
}

// ---------------------------------------------------------------------------
// Port forwarding
// ---------------------------------------------------------------------------

// forward accepts connections on the local listener and proxies them
// through the SSH tunnel to the remote address.
func (t *SSHTunnel) forward() {
	remoteAddr := fmt.Sprintf("%s:%d", t.config.RemoteHost, t.config.RemotePort)

	for {
		localConn, err := t.listener.Accept()
		if err != nil {
			// Listener closed (normal shutdown)
			return
		}

		go func() {
			defer func() { _ = localConn.Close() }()

			t.mu.Lock()
			client := t.client
			t.mu.Unlock()

			if client == nil {
				return
			}

			remoteConn, err := client.Dial("tcp", remoteAddr)
			if err != nil {
				logger.L().Error("tunnel: failed to dial remote",
					"remote_addr", remoteAddr,
					"error", err,
				)
				return
			}
			defer func() { _ = remoteConn.Close() }()

			// Bidirectional copy
			go copyConn(localConn, remoteConn)
			copyConn(remoteConn, localConn)
		}()
	}
}

// copyConn copies data from dst to src.
func copyConn(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Keepalive and health
// ---------------------------------------------------------------------------

// keepalive sends periodic keepalive requests to the SSH server.
// If the server becomes unreachable, the tunnel attempts to reconnect
// before marking itself as permanently closed.
func (t *SSHTunnel) keepalive() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	consecutiveFailures := 0
	const maxFailures = 3

	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			t.mu.Lock()
			client := t.client
			t.mu.Unlock()

			if client == nil {
				return
			}

			_, _, err := client.SendRequest("keepalive@golang.org", true, nil)
			if err != nil {
				consecutiveFailures++
				logger.L().Warn("tunnel: keepalive failed",
					"ssh_host", t.config.Host,
					"consecutive_failures", consecutiveFailures,
					"error", err,
				)

				if consecutiveFailures >= maxFailures {
					if t.reconnect() {
						consecutiveFailures = 0
					} else {
						logger.L().Error("tunnel: SSH connection lost (reconnect failed)",
							"ssh_host", t.config.Host,
							"failures", consecutiveFailures,
						)
						t.mu.Lock()
						t.open = false
						t.mu.Unlock()
						return
					}
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}

// reconnect attempts to re-establish the SSH connection and listener.
// It tries to rebind the same local port so that existing database driver
// connections (which are wired to the old local address) remain valid.
// Returns true on success. The tunnel remains closed on failure.
func (t *SSHTunnel) reconnect() bool {
	log := logger.L().With("component", "tunnel", "ssh_host", t.config.Host)
	log.Warn("tunnel: attempting reconnect")

	sshConfig, err := t.buildSSHConfig()
	if err != nil {
		log.Error("tunnel: reconnect failed (auth config)", "error", err)
		return false
	}

	addr := fmt.Sprintf("%s:%d", t.config.Host, t.config.Port)
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		log.Error("tunnel: reconnect failed (dial)", "error", err)
		return false
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, sshConfig)
	if err != nil {
		_ = conn.Close()
		log.Error("tunnel: reconnect failed (handshake)", "error", err)
		return false
	}

	newClient := ssh.NewClient(sshConn, chans, reqs)

	// Snapshot the old local address and resources under lock.
	t.mu.Lock()
	oldListener := t.listener
	oldClient := t.client
	previousAddr := t.localAddr
	t.client = newClient
	t.mu.Unlock()

	// Signal old keepalive and forwarding goroutines to stop.
	close(t.done)
	t.done = make(chan struct{})

	if oldListener != nil {
		_ = oldListener.Close()
	}
	if oldClient != nil {
		_ = oldClient.Close()
	}

	// Try to rebind the same local port so existing DB connections survive.
	newListener, newAddr := t.listenReuse(previousAddr)
	if newListener == nil {
		log.Error("tunnel: reconnect failed (listen)", "error", err)
		t.mu.Lock()
		t.open = false
		t.mu.Unlock()
		return false
	}

	t.mu.Lock()
	t.listener = newListener
	t.localAddr = newAddr
	t.mu.Unlock()

	// Restart forwarding goroutine
	go t.forward()

	// Restart keepalive for the new connection.
	go t.keepalive()

	log.Debug("tunnel: reconnected successfully", "local_addr", t.localAddr)
	return true
}

// listenReuse tries to bind the previous local address so existing DB
// connections remain valid. Falls back to a random port if the old address
// is no longer available.
func (t *SSHTunnel) listenReuse(previousAddr string) (net.Listener, string) {
	if previousAddr != "" {
		if ln, err := net.Listen("tcp", previousAddr); err == nil {
			return ln, previousAddr
		}
		// Old port taken (unlikely but possible); fall through to random.
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, ""
	}
	return ln, ln.Addr().String()
}
