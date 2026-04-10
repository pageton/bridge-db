package provider

import "github.com/bytedance/sonic"

// UnmarshalScanToken deserializes a resume token into ScanStats.
// Returns a zero ScanStats for empty input.
func UnmarshalScanToken(token []byte) (ScanStats, error) {
	if len(token) == 0 {
		return ScanStats{}, nil
	}
	var m map[string]int64
	if err := sonic.Unmarshal(token, &m); err != nil {
		return ScanStats{}, err
	}
	return ScanStats{
		TotalScanned: m["total_scanned"],
		TotalBytes:   m["total_bytes"],
		TablesDone:   int(m["tables_done"]),
		TablesTotal:  int(m["tables_total"]),
	}, nil
}
