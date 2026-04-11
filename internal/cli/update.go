package cli

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

var (
	updateCheck  bool
	updateTarget string
	updateForce  bool
)

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update bridge to the latest version",
	Long: `Check for and install the latest version of bridge from GitHub Releases.

The command:
  1. Queries the GitHub Releases API for the latest version
  2. Compares with the current binary version
  3. Downloads the correct binary for this OS/architecture
  4. Verifies the SHA-256 checksum
  5. Replaces the running binary in-place

Examples:
  bridge update                  # update to the latest version
  bridge update --check          # only check, do not install
  bridge update --version v1.2.0 # update to a specific version
  bridge update --force          # reinstall even if already up-to-date`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runUpdate,
}

func init() {
	updateCmd.Flags().BoolVar(&updateCheck, "check", false, "only check for updates, do not install")
	updateCmd.Flags().StringVar(&updateTarget, "version", "", "update to a specific version instead of latest")
	updateCmd.Flags().BoolVar(&updateForce, "force", false, "update even if already at the target version")
	rootCmd.AddCommand(updateCmd)
}

func runUpdate(cmd *cobra.Command, args []string) error {
	current := Version
	if current == "" {
		current = "dev"
	}

	// Step 1: Resolve target release.
	var release *githubRelease
	var err error

	if updateTarget != "" {
		v := updateTarget
		if !strings.HasPrefix(v, "v") {
			v = "v" + v
		}
		fmt.Printf("Fetching release %s...\n", v)
		release, err = fetchReleaseByVersion(v)
		if err != nil {
			return fmt.Errorf("fetch release %s: %w", v, err)
		}
	} else {
		fmt.Println("Checking for updates...")
		release, err = fetchLatestRelease()
		if err != nil {
			return fmt.Errorf("check for updates: %w", err)
		}
	}

	targetVersion := release.TagName

	// Step 2: --check mode.
	if updateCheck {
		if targetVersion == current {
			fmt.Printf("Already up-to-date: %s\n", current)
		} else {
			fmt.Printf("Update available: %s -> %s\n", current, targetVersion)
		}
		return nil
	}

	// Step 3: Skip if already up-to-date.
	if targetVersion == current && !updateForce {
		fmt.Printf("Already up-to-date: %s\n", current)
		return nil
	}

	if !updateForce && !compareVersions(current, targetVersion) {
		fmt.Printf("Current version %s is newer than target %s. Use --force to downgrade.\n", current, targetVersion)
		return nil
	}

	// Step 4: Resolve asset for this platform.
	assetName := assetNameForPlatform(targetVersion)
	downloadURL, err := findAssetURL(release, assetName)
	if err != nil {
		return fmt.Errorf("find binary for %s/%s: %w", runtime.GOOS, runtime.GOARCH, err)
	}

	checksumsURL, checksumErr := findChecksumsURL(release)
	if checksumErr != nil {
		fmt.Fprintln(os.Stderr, "Warning: checksums.txt not found — skipping verification")
	}

	// Step 5: Download.
	fmt.Printf("Downloading %s...\n", assetName)
	tmpBinary, err := downloadFile(downloadURL)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer func() { _ = os.Remove(tmpBinary) }()

	// Step 6: Verify checksum.
	if checksumErr == nil {
		fmt.Println("Verifying checksum...")
		if err := verifyChecksum(tmpBinary, assetName, checksumsURL); err != nil {
			return fmt.Errorf("checksum verification failed: %w", err)
		}
	}

	// Step 7: Replace.
	fmt.Println("Installing...")
	if err := selfReplace(tmpBinary); err != nil {
		return fmt.Errorf("replace binary: %w", err)
	}

	fmt.Printf("Updated bridge %s -> %s\n", current, targetVersion)
	return nil
}
