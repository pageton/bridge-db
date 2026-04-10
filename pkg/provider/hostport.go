package provider

import "fmt"

// ParseHostPort parses a host:port string, handling IPv6 bracket notation.
// If no port is found, defaultPort is returned.
func ParseHostPort(addr string, defaultPort int) (string, int, error) {
	if len(addr) == 0 {
		return "", 0, fmt.Errorf("empty address")
	}

	var host string
	var port int

	if addr[0] == '[' {
		// IPv6 format: [::1]:5432
		end := -1
		for i := 1; i < len(addr); i++ {
			if addr[i] == ']' {
				end = i
				break
			}
		}
		if end == -1 {
			return "", 0, fmt.Errorf("invalid IPv6 address")
		}
		host = addr[1:end]
		if end+1 < len(addr) && addr[end+1] == ':' {
			_, _ = fmt.Sscanf(addr[end+2:], "%d", &port)
		}
	} else {
		// IPv4 or hostname
		for i := len(addr) - 1; i >= 0; i-- {
			if addr[i] == ':' {
				host = addr[:i]
				_, _ = fmt.Sscanf(addr[i+1:], "%d", &port)
				break
			}
		}
		if host == "" {
			host = addr
		}
	}

	if port == 0 {
		port = defaultPort
	}

	return host, port, nil
}
