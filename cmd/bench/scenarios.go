package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/pageton/bridge-db/pkg/provider"
)

// ---------------------------------------------------------------------------
// Provider endpoints
// ---------------------------------------------------------------------------

// ProviderEndpoint describes a database endpoint for benchmarking.
type ProviderEndpoint struct {
	Provider string // e.g. "sqlite", "postgres", "mongodb"
	URL      string // connection URL (blank for sqlite temp files)
	Tag      string // build tag required, e.g. "sqlite", "mongodb"
}

// DisplayName returns "provider" or "provider(url)" for logging.
func (e ProviderEndpoint) DisplayName() string {
	if e.URL != "" {
		return fmt.Sprintf("%s", e.Provider)
	}
	return e.Provider
}

// IsSQLite returns true for the zero-dependency SQLite provider.
func (e ProviderEndpoint) IsSQLite() bool {
	return e.Provider == "sqlite"
}

// ---------------------------------------------------------------------------
// Default endpoints (matching example/flake.nix ports)
// ---------------------------------------------------------------------------

// DefaultEndpoints returns the default connection endpoints for all providers.
// These match the ports in example/flake.nix.
func DefaultEndpoints() map[string]ProviderEndpoint {
	return map[string]ProviderEndpoint{
		"sqlite":          {Provider: "sqlite", Tag: "sqlite"},
		"postgres":        {Provider: "postgres", URL: "postgresql://127.0.0.1:5432/testdb?sslmode=disable", Tag: ""},
		"postgres-dst":    {Provider: "postgres", URL: "postgresql://127.0.0.1:5433/testdb?sslmode=disable", Tag: ""},
		"mysql":           {Provider: "mysql", URL: "mysql://root@127.0.0.1:3306/testdb", Tag: ""},
		"mysql-dst":       {Provider: "mysql", URL: "mysql://root@127.0.0.1:3307/testdb", Tag: ""},
		"mariadb":         {Provider: "mariadb", URL: "mariadb://root@127.0.0.1:3308/testdb", Tag: ""},
		"mariadb-dst":     {Provider: "mariadb", URL: "mariadb://root@127.0.0.1:3309/testdb", Tag: ""},
		"cockroachdb":     {Provider: "cockroachdb", URL: "cockroachdb://root@localhost:26257/testdb?sslmode=disable", Tag: ""},
		"cockroachdb-dst": {Provider: "cockroachdb", URL: "cockroachdb://root@localhost:26258/testdb?sslmode=disable", Tag: ""},
		"mongodb":         {Provider: "mongodb", URL: "mongodb://127.0.0.1:27017/testdb", Tag: "mongodb"},
		"mongodb-dst":     {Provider: "mongodb", URL: "mongodb://127.0.0.1:27018/testdb", Tag: "mongodb"},
		"redis":           {Provider: "redis", URL: "redis://127.0.0.1:6379/0", Tag: "redis"},
		"redis-dst":       {Provider: "redis", URL: "redis://127.0.0.1:6380/1", Tag: "redis"},
		"mssql":           {Provider: "mssql", URL: "mssql://sa:BridgeDb123!@localhost:1433/testdb?TrustServerCertificate=true", Tag: "mssql"},
		"mssql-dst":       {Provider: "mssql", URL: "mssql://sa:BridgeDb123!@localhost:1434/testdb?TrustServerCertificate=true", Tag: "mssql"},
	}
}

// ---------------------------------------------------------------------------
// Scenario parsing
// ---------------------------------------------------------------------------

// Scenario represents a source→destination provider pair to benchmark.
type Scenario struct {
	Source      ProviderEndpoint
	Destination ProviderEndpoint
}

// Label returns a human-readable label like "sqlite→postgres".
func (s Scenario) Label() string {
	return fmt.Sprintf("%s→%s", s.Source.Provider, s.Destination.Provider)
}

// ParseEndpoint parses a provider specification from a flag value.
// Formats: "provider" (uses default URL), "provider:url" (custom URL).
func ParseEndpoint(spec string, defaults map[string]ProviderEndpoint, destination bool) (ProviderEndpoint, error) {
	parts := strings.SplitN(spec, ":", 2)
	name := parts[0]
	var customURL string
	if len(parts) == 2 {
		customURL = parts[1]
	}

	ep, ok := endpointFor(name, defaults, destination)
	if !ok {
		return ProviderEndpoint{}, fmt.Errorf("unknown provider %q (available: %s)", name, availableProviders(defaults))
	}

	if customURL != "" {
		ep.URL = customURL
	}
	return ep, nil
}

// ParseScenario parses "source→dest" or "source,dest" format.
func ParseScenario(spec string, defaults map[string]ProviderEndpoint) (Scenario, error) {
	// Support both → and , as separators
	spec = strings.ReplaceAll(spec, "→", ",")
	spec = strings.ReplaceAll(spec, "->", ",")

	parts := strings.SplitN(spec, ",", 2)
	if len(parts) != 2 {
		return Scenario{}, fmt.Errorf("scenario must be \"source→dest\" or \"source,dest\", got %q", spec)
	}

	src, err := ParseEndpoint(strings.TrimSpace(parts[0]), defaults, false)
	if err != nil {
		return Scenario{}, fmt.Errorf("source: %w", err)
	}
	dst, err := ParseEndpoint(strings.TrimSpace(parts[1]), defaults, true)
	if err != nil {
		return Scenario{}, fmt.Errorf("destination: %w", err)
	}

	return Scenario{Source: src, Destination: dst}, nil
}

// ParseScenarios parses a comma-or-semicolon separated list of scenarios.
// Each scenario is "source→dest" or "source,dest".
func ParseScenarios(spec string, defaults map[string]ProviderEndpoint) ([]Scenario, error) {
	var scenarios []Scenario
	for _, s := range strings.Split(spec, ";") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		sc, err := ParseScenario(s, defaults)
		if err != nil {
			return nil, err
		}
		scenarios = append(scenarios, sc)
	}
	return scenarios, nil
}

// ExpandScenarioGroups expands named scenario groups into individual scenarios.
// Groups: "sql-same", "sql-cross", "nosql-same", "nosql-cross", "all-same", "all-cross", "all".
func ExpandScenarioGroups(groups []string) ([]Scenario, error) {
	defaults := DefaultEndpoints()
	sqlProviders := []string{"sqlite", "postgres", "mysql", "mariadb", "cockroachdb", "mssql"}
	nosqlProviders := []string{"mongodb", "redis"}
	allProviders := append(sqlProviders, nosqlProviders...)

	var scenarios []Scenario
	seen := make(map[string]bool)

	add := func(src, dst string) {
		key := src + "→" + dst
		if seen[key] {
			return
		}
		srcEp, ok := endpointFor(src, defaults, false)
		if !ok {
			return
		}
		dstEp, ok := endpointFor(dst, defaults, true)
		if !ok {
			return
		}
		seen[key] = true
		scenarios = append(scenarios, Scenario{
			Source:      srcEp,
			Destination: dstEp,
		})
	}

	for _, g := range groups {
		switch g {
		case "sql-same":
			for _, p := range sqlProviders {
				add(p, p)
			}
		case "sql-cross":
			for _, p := range sqlProviders {
				for _, q := range sqlProviders {
					if p != q {
						add(p, q)
					}
				}
			}
		case "nosql-same":
			for _, p := range nosqlProviders {
				add(p, p)
			}
		case "nosql-cross":
			for _, p := range nosqlProviders {
				for _, q := range nosqlProviders {
					if p != q {
						add(p, q)
					}
				}
			}
		case "all-same":
			for _, p := range allProviders {
				add(p, p)
			}
		case "all-cross":
			for _, p := range allProviders {
				for _, q := range allProviders {
					if p != q {
						add(p, q)
					}
				}
			}
		case "all":
			for _, p := range allProviders {
				for _, q := range allProviders {
					add(p, q)
				}
			}
		default:
			// Try as explicit "source→dest" scenario
			sc, err := ParseScenario(g, defaults)
			if err != nil {
				return nil, fmt.Errorf("unknown group or invalid scenario %q: %w", g, err)
			}
			add(sc.Source.Provider, sc.Destination.Provider)
		}
	}

	return scenarios, nil
}

// availableProviders returns a comma-separated list of provider names.
func availableProviders(defaults map[string]ProviderEndpoint) string {
	seen := make(map[string]bool)
	var names []string
	for k := range defaults {
		// Strip -dst suffix for display
		name := strings.TrimSuffix(k, "-dst")
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return strings.Join(names, ", ")
}

func endpointFor(name string, defaults map[string]ProviderEndpoint, destination bool) (ProviderEndpoint, bool) {
	if destination {
		if ep, ok := defaults[name+"-dst"]; ok {
			return ep, true
		}
	}
	ep, ok := defaults[name]
	return ep, ok
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

// CreateSourceDB returns a database name for the source provider.
// For SQLite, it returns a temp file path. For others, it uses the URL from the endpoint.
func CreateSourceDBName(tmpDir string) string {
	return "file://" + tmpDir + "/source.db"
}

// CreateDestDBName returns a database name for the destination provider.
// For SQLite, it returns a temp file path. For others, it uses the URL from the endpoint.
func CreateDestDBName(tmpDir string) string {
	return "file://" + tmpDir + "/dest.db"
}

// SourceURL returns the connection URL for a source endpoint in a temp dir.
// For SQLite, creates a file path. For others, uses the endpoint URL.
func SourceURL(ep ProviderEndpoint, tmpDir string) string {
	if ep.IsSQLite() {
		return tmpDir + "/source.db"
	}
	return ep.URL
}

// DestURL returns the connection URL for a destination endpoint in a temp dir.
func DestURL(ep ProviderEndpoint, tmpDir string) string {
	if ep.IsSQLite() {
		return tmpDir + "/dest.db"
	}
	return ep.URL
}

// NeedsSourceEndpoint returns true if the source provider needs a real DB
// connection (not a temp SQLite file).
func NeedsSourceEndpoint(ep ProviderEndpoint) bool {
	return !ep.IsSQLite()
}

// CheckProviderAvailable verifies a provider is compiled in.
func CheckProviderAvailable(name string) bool {
	for _, p := range provider.AvailableProviders() {
		if p == name {
			return true
		}
	}
	return false
}

// WarnUnavailable prints warnings for providers that aren't compiled in.
func WarnUnavailable(scenarios []Scenario) {
	missing := make(map[string]bool)
	for _, sc := range scenarios {
		if !CheckProviderAvailable(sc.Source.Provider) {
			missing[sc.Source.Provider] = true
		}
		if !CheckProviderAvailable(sc.Destination.Provider) {
			missing[sc.Destination.Provider] = true
		}
	}
	for p := range missing {
		fmt.Fprintf(os.Stderr, "  WARNING: provider %q is not compiled in (missing build tag)\n", p)
	}
}

// FilterAvailable filters scenarios to only those where both providers are compiled in.
func FilterAvailable(scenarios []Scenario) []Scenario {
	var out []Scenario
	for _, sc := range scenarios {
		if CheckProviderAvailable(sc.Source.Provider) && CheckProviderAvailable(sc.Destination.Provider) {
			out = append(out, sc)
		}
	}
	return out
}
