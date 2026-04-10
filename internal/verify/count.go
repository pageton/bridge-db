package verify

import "github.com/pageton/bridge-db/pkg/provider"

// ToVerificationErrors converts a VerificationReport to provider.VerificationErrors.
func ToVerificationErrors(report *VerificationReport) []provider.VerificationError {
	errs := make([]provider.VerificationError, 0, len(report.Mismatches))
	for _, m := range report.Mismatches {
		errs = append(errs, provider.VerificationError{
			Key:     m.Key,
			Table:   m.Table,
			Message: m.String(),
		})
	}
	return errs
}
