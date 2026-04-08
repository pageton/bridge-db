package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify data integrity between source and destination",
	Long: `Verify that data was correctly migrated by comparing row counts,
document counts, or key counts between source and destination databases.
Optionally sample and compare individual records.`,
	RunE: runVerify,
}

func init() {
	rootCmd.AddCommand(verifyCmd)
}

func runVerify(cmd *cobra.Command, args []string) error {
	fmt.Println("Use --verify flag with migrate command for post-migration verification.")
	return nil
}
