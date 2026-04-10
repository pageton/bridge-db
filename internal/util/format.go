package util

import (
	"fmt"
	"time"
)

// HumanBytes converts a byte count to a human-readable string (e.g. "1.5 GiB").
func HumanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// Truncate shortens s to max length, appending "..." if truncated.
func Truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max > 3 {
		return s[:max-3] + "..."
	}
	return s[:max]
}

// FormatDuration returns a human-readable duration string with
// sub-millisecond precision. It avoids showing "0ms" for fast operations
// by using microsecond or decimal-millisecond formatting.
func FormatDuration(d time.Duration) string {
	if d < 0 {
		return "?"
	}
	if d < time.Microsecond {
		return "<1µs"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < 100*time.Millisecond {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		s := d.Seconds()
		if s == float64(int(s)) {
			return fmt.Sprintf("%ds", int(s))
		}
		return fmt.Sprintf("%.1fs", s)
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}
