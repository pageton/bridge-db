//go:build !sqlite

package main

import (
	"context"
	"fmt"
)

// SeedProvider returns an error when built without the sqlite tag.
// The benchmark seeder requires sqlite for generating source data.
func SeedProvider(_ context.Context, ep ProviderEndpoint, _ string, _ DatasetSizeConfig, _ int64, _ int, _ int) (string, int64, error) {
	return "", 0, fmt.Errorf("benchmark seeding requires sqlite build tag (rebuild with -tags sqlite)")
}
