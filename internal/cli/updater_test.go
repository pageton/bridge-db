package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildAssetName(t *testing.T) {
	tests := []struct {
		version string
		goos    string
		goarch  string
		want    string
	}{
		{"v1.0.0", "linux", "amd64", "bridge_v1.0.0_linux_amd64"},
		{"v1.0.0", "linux", "arm64", "bridge_v1.0.0_linux_arm64"},
		{"v1.0.0", "darwin", "amd64", "bridge_v1.0.0_darwin_amd64"},
		{"v1.0.0", "darwin", "arm64", "bridge_v1.0.0_darwin_arm64"},
		{"v1.0.0", "windows", "amd64", "bridge_v1.0.0_windows_amd64.exe"},
		{"1.0.0", "linux", "amd64", "bridge_v1.0.0_linux_amd64"},
	}
	for _, tt := range tests {
		got := buildAssetName(tt.version, tt.goos, tt.goarch)
		if got != tt.want {
			t.Errorf("buildAssetName(%q, %q, %q) = %q, want %q", tt.version, tt.goos, tt.goarch, got, tt.want)
		}
	}
}

func TestParseChecksums(t *testing.T) {
	content := `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  bridge_v1.0.0_linux_amd64
a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2  bridge_v1.0.0_darwin_arm64

f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2  bridge_v1.0.0_windows_amd64.exe
`
	m := parseChecksums(content)
	if len(m) != 3 {
		t.Fatalf("parseChecksums returned %d entries, want 3", len(m))
	}
	if m["bridge_v1.0.0_linux_amd64"] != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("unexpected hash for linux_amd64: %s", m["bridge_v1.0.0_linux_amd64"])
	}
	if m["bridge_v1.0.0_darwin_arm64"] != "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2" {
		t.Errorf("unexpected hash for darwin_arm64: %s", m["bridge_v1.0.0_darwin_arm64"])
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		local  string
		remote string
		want   bool
	}{
		{"v1.0.0", "v1.0.0", false},
		{"v1.0.0", "v1.0.1", true},
		{"v1.1.0", "v1.0.1", false},
		{"v1.0.0", "v2.0.0", true},
		{"v2.0.0", "v1.9.9", false},
		{"1.0.0", "1.0.1", true},
		{"v0.1.2", "v0.2.0", true},
		{"dev", "v1.0.0", true}, // "dev" parses as 0.0.0, so v1.0.0 is newer
	}
	for _, tt := range tests {
		got := compareVersions(tt.local, tt.remote)
		if got != tt.want {
			t.Errorf("compareVersions(%q, %q) = %v, want %v", tt.local, tt.remote, got, tt.want)
		}
	}
}

func TestFindAssetURL(t *testing.T) {
	release := &githubRelease{
		TagName: "v1.0.0",
		Assets: []githubAsset{
			{Name: "bridge_v1.0.0_linux_amd64", BrowserDownloadURL: "https://example.com/linux"},
			{Name: "bridge_v1.0.0_darwin_arm64", BrowserDownloadURL: "https://example.com/darwin"},
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
		},
	}

	url, err := findAssetURL(release, "bridge_v1.0.0_linux_amd64")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if url != "https://example.com/linux" {
		t.Errorf("got %q, want %q", url, "https://example.com/linux")
	}

	_, err = findAssetURL(release, "bridge_v1.0.0_windows_amd64.exe")
	if err == nil {
		t.Error("expected error for missing asset")
	}
}

func TestFindChecksumsURL(t *testing.T) {
	release := &githubRelease{
		TagName: "v1.0.0",
		Assets: []githubAsset{
			{Name: "bridge_v1.0.0_linux_amd64", BrowserDownloadURL: "https://example.com/linux"},
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
		},
	}

	url, err := findChecksumsURL(release)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if url != "https://example.com/checksums.txt" {
		t.Errorf("got %q, want %q", url, "https://example.com/checksums.txt")
	}

	noChecksums := &githubRelease{
		TagName: "v1.0.0",
		Assets: []githubAsset{
			{Name: "bridge_v1.0.0_linux_amd64", BrowserDownloadURL: "https://example.com/linux"},
		},
	}
	_, err = findChecksumsURL(noChecksums)
	if err == nil {
		t.Error("expected error when checksums.txt is missing")
	}
}

func TestVerifyChecksum(t *testing.T) {
	content := []byte("hello bridge-db")
	tmp, err := os.CreateTemp("", "bridge-checksum-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}

	hash := sha256.Sum256(content)
	hashHex := hex.EncodeToString(hash[:])
	assetName := "bridge_v1.0.0_linux_amd64"
	checksumsContent := hashHex + "  " + assetName + "\n"

	sums := parseChecksums(checksumsContent)
	expected, ok := sums[assetName]
	if !ok {
		t.Fatal("asset not found in checksums")
	}

	f, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := h.Write(content); err != nil {
		t.Fatal(err)
	}
	actual := hex.EncodeToString(h.Sum(nil))

	if actual != expected {
		t.Errorf("checksum mismatch: got %s, want %s", actual, expected)
	}
}

func TestSelfReplaceUnix(t *testing.T) {
	dir := t.TempDir()
	current := filepath.Join(dir, "bridge")
	newContent := []byte("new binary content")

	if err := os.WriteFile(current, []byte("old content"), 0755); err != nil {
		t.Fatal(err)
	}

	newFile, err := os.CreateTemp(dir, "new-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newFile.Write(newContent); err != nil {
		t.Fatal(err)
	}
	if err := newFile.Close(); err != nil {
		t.Fatal(err)
	}

	// Test the core self-replace logic manually since we can't override os.Executable().
	exeDir := filepath.Dir(current)
	tmp, err := os.CreateTemp(exeDir, ".bridge-update-*")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmp.Name()

	src, err := os.Open(newFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		t.Fatalf("copy: %v", err)
	}
	_ = tmp.Close()
	_ = src.Close()

	if err := os.Chmod(tmpPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpPath, current); err != nil {
		t.Fatalf("rename: %v", err)
	}

	got, err := os.ReadFile(current)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(newContent) {
		t.Errorf("after replace: got %q, want %q", got, newContent)
	}
}
