package verify

import (
	"context"
	"fmt"

	"github.com/pageton/bridge-db/pkg/provider"
)

// compareChecksums compares row-level checksums between source and destination
// and records mismatches into the TableResult.
func compareChecksums(
	_ context.Context,
	srcChecksum, dstChecksum provider.Checksummer,
	table string,
	keys []string,
	tr *TableResult,
) {
	srcHashes, err := srcChecksum.ComputeChecksums(context.Background(), keys)
	if err != nil {
		tr.Mismatches = append(tr.Mismatches, MismatchDetail{
			Category: MismatchChecksumDiff,
			Table:    table,
			Message:  fmt.Sprintf("source checksum failed: %v", err),
		})
		tr.SampleMismatch++
		return
	}

	dstHashes, err := dstChecksum.ComputeChecksums(context.Background(), keys)
	if err != nil {
		tr.Mismatches = append(tr.Mismatches, MismatchDetail{
			Category: MismatchChecksumDiff,
			Table:    table,
			Message:  fmt.Sprintf("destination checksum failed: %v", err),
		})
		tr.SampleMismatch++
		return
	}

	for _, key := range keys {
		srcHash, srcOK := srcHashes[key]
		dstHash, dstOK := dstHashes[key]

		if srcOK && !dstOK {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category:    MismatchMissingInDst,
				Table:       table,
				Key:         key,
				SrcChecksum: srcHash,
				Message:     fmt.Sprintf("key %q not found in destination", key),
			})
			tr.SampleMismatch++
			continue
		}

		if !srcOK && dstOK {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category:    MismatchExtraInDst,
				Table:       table,
				Key:         key,
				DstChecksum: dstHash,
				Message:     fmt.Sprintf("key %q exists in destination but not in source", key),
			})
			tr.SampleMismatch++
			continue
		}

		if srcHash != dstHash {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category:    MismatchChecksumDiff,
				Table:       table,
				Key:         key,
				SrcChecksum: srcHash,
				DstChecksum: dstHash,
				Message:     fmt.Sprintf("key %q checksum mismatch", key),
			})
			tr.SampleMismatch++
		}
	}
}
