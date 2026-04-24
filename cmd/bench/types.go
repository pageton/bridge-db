package main

// DatasetSizeConfig defines how many records to generate and expected data size.
type DatasetSizeConfig struct {
	Name     string
	Records  int
	Tables   int
	BytesEst int64 // approximate expected data size in bytes
	WideRows bool  // use wider rows with more columns
}

// DatasetSizePresets maps size names to configurations.
var DatasetSizePresets = map[string]DatasetSizeConfig{
	"small": {
		Name:     "small",
		Records:  10_000,
		Tables:   3,
		BytesEst: 2 * 1024 * 1024, // ~2 MB
		WideRows: false,
	},
	"medium": {
		Name:     "medium",
		Records:  100_000,
		Tables:   5,
		BytesEst: 20 * 1024 * 1024, // ~20 MB
		WideRows: false,
	},
	"large": {
		Name:     "large",
		Records:  1_000_000,
		Tables:   5,
		BytesEst: 200 * 1024 * 1024, // ~200 MB
		WideRows: true,
	},
}

// DatasetSizeByName looks up a preset by name.
func DatasetSizeByName(name string) (DatasetSizeConfig, bool) {
	cfg, ok := DatasetSizePresets[name]
	return cfg, ok
}
