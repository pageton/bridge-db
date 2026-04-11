package cli

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	repoOwner = "pageton"
	repoName  = "bridge-db"
	appName   = "bridge"

	githubAPI = "https://api.github.com/repos/" + repoOwner + "/" + repoName
)

// githubRelease represents the subset of the GitHub Releases API response we need.
type githubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []githubAsset `json:"assets"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// fetchLatestRelease queries the GitHub Releases API for the latest release.
func fetchLatestRelease() (*githubRelease, error) {
	return fetchRelease(githubAPI + "/releases/latest")
}

// fetchReleaseByVersion fetches a specific release by tag name.
func fetchReleaseByVersion(version string) (*githubRelease, error) {
	v := version
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	return fetchRelease(githubAPI + "/releases/tags/" + v)
}

func fetchRelease(url string) (*githubRelease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &release, nil
}

// buildAssetName produces the binary asset name for a given OS/arch pair.
// Mirrors the naming convention from .github/workflows/release.yml.
func buildAssetName(version, goos, goarch string) string {
	v := version
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	name := fmt.Sprintf("%s_%s_%s_%s", appName, v, goos, goarch)
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

func assetNameForPlatform(version string) string {
	return buildAssetName(version, runtime.GOOS, runtime.GOARCH)
}

func findAssetURL(release *githubRelease, assetName string) (string, error) {
	for _, a := range release.Assets {
		if a.Name == assetName {
			return a.BrowserDownloadURL, nil
		}
	}
	return "", fmt.Errorf("no asset named %s in release %s", assetName, release.TagName)
}

func findChecksumsURL(release *githubRelease) (string, error) {
	for _, a := range release.Assets {
		if a.Name == "checksums.txt" {
			return a.BrowserDownloadURL, nil
		}
	}
	return "", fmt.Errorf("checksums.txt not found in release %s", release.TagName)
}

func downloadFile(url string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}

	tmp, err := os.CreateTemp("", "bridge-update-*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("write download: %w", err)
	}
	_ = tmp.Close()

	return tmp.Name(), nil
}

func parseChecksums(content string) map[string]string {
	m := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) == 2 {
			m[parts[1]] = parts[0]
		}
	}
	return m
}

func verifyChecksum(binaryPath, assetName, checksumsURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checksumsURL, nil)
	if err != nil {
		return fmt.Errorf("build checksum request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download checksums: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read checksums: %w", err)
	}

	checksums := parseChecksums(string(body))
	expected, ok := checksums[assetName]
	if !ok {
		return fmt.Errorf("no checksum entry for %s", assetName)
	}

	f, err := os.Open(binaryPath)
	if err != nil {
		return fmt.Errorf("open binary: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hash binary: %w", err)
	}

	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expected {
		return fmt.Errorf("expected %s, got %s", expected, actual)
	}
	return nil
}

// compareVersions returns true if remote is newer than local.
func compareVersions(local, remote string) bool {
	l := strings.TrimPrefix(local, "v")
	r := strings.TrimPrefix(remote, "v")

	lParts := strings.SplitN(l, ".", 3)
	rParts := strings.SplitN(r, ".", 3)

	for i := range 3 {
		lp, rr := 0, 0
		if i < len(lParts) {
			lp, _ = strconv.Atoi(lParts[i])
		}
		if i < len(rParts) {
			rr, _ = strconv.Atoi(rParts[i])
		}
		if rr > lp {
			return true
		}
		if rr < lp {
			return false
		}
	}
	return false
}

func currentExecutable() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	return filepath.EvalSymlinks(exe)
}

// selfReplace atomically replaces the running binary with newBinaryPath.
func selfReplace(newBinaryPath string) error {
	if runtime.GOOS == "windows" {
		return selfReplaceWindows(newBinaryPath)
	}
	return selfReplaceUnix(newBinaryPath)
}

func selfReplaceUnix(newBinaryPath string) error {
	exePath, err := currentExecutable()
	if err != nil {
		return err
	}
	exeDir := filepath.Dir(exePath)

	tmp, err := os.CreateTemp(exeDir, ".bridge-update-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	src, err := os.Open(newBinaryPath)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("open new binary: %w", err)
	}
	defer func() { _ = src.Close() }()

	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("copy binary: %w", err)
	}
	_ = tmp.Close()

	if err := os.Chmod(tmpPath, 0755); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}

	if err := os.Rename(tmpPath, exePath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

func selfReplaceWindows(newBinaryPath string) error {
	exePath, err := currentExecutable()
	if err != nil {
		return err
	}

	backup := exePath + ".bak"
	if err := os.Rename(exePath, backup); err != nil {
		return fmt.Errorf("rename old binary: %w", err)
	}

	src, err := os.Open(newBinaryPath)
	if err != nil {
		_ = os.Rename(backup, exePath)
		return fmt.Errorf("open new binary: %w", err)
	}
	defer func() { _ = src.Close() }()

	dst, err := os.Create(exePath)
	if err != nil {
		_ = os.Rename(backup, exePath)
		return fmt.Errorf("create new binary: %w", err)
	}
	defer func() { _ = dst.Close() }()

	if _, err := io.Copy(dst, src); err != nil {
		_ = os.Rename(backup, exePath)
		return fmt.Errorf("copy new binary: %w", err)
	}

	_ = os.Remove(backup)
	return nil
}
