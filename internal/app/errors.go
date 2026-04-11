package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/bridge"
)

// StructuredError is the stable MCP-facing error contract for service and tool
// responses.
type StructuredError struct {
	Code            string            `json:"code,omitempty"`
	Category        string            `json:"category"`
	Phase           string            `json:"phase,omitempty"`
	Provider        string            `json:"provider,omitempty"`
	ProviderRole    string            `json:"provider_role,omitempty"`
	Retryable       bool              `json:"retryable"`
	HumanMessage    string            `json:"human_message"`
	TechnicalDetail string            `json:"technical_detail,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// ErrorFrom converts internal errors into the stable structured error model.
func ErrorFrom(err error, fallbackCategory, phase, providerName, providerRole string) *StructuredError {
	if err == nil {
		return nil
	}

	out := &StructuredError{
		Category:        fallbackCategory,
		Phase:           phase,
		Provider:        providerName,
		ProviderRole:    providerRole,
		Retryable:       false,
		HumanMessage:    err.Error(),
		TechnicalDetail: err.Error(),
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		out.Category = string(bridge.ErrCancelled)
		out.Phase = "cancelled"
		out.Code = "BRIDGE_CANCELLED"
		return out
	}

	var ce *bridge.CategorizedError
	if errors.As(err, &ce) {
		out.Category = string(ce.Category)
		out.Phase = ce.Phase
		out.Retryable = ce.Retryable
		out.HumanMessage = ce.Message
		out.TechnicalDetail = ce.Error()
		out.Code = codeForCategory(ce.Category)
		return out
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "checkpoint"):
		out.Category = "checkpoint"
		out.Code = "BRIDGE_CHECKPOINT_ERROR"
	case strings.Contains(lower, "resume") || strings.Contains(lower, "config hash mismatch"):
		out.Category = "resume"
		out.Code = "BRIDGE_RESUME_ERROR"
	case strings.Contains(lower, "unknown run_id"):
		out.Category = "internal"
		out.Code = "BRIDGE_RUN_NOT_FOUND"
		out.HumanMessage = fmt.Sprintf("Migration run could not be found: %s", err.Error())
	case strings.Contains(lower, "unknown provider"):
		out.Category = string(bridge.ErrConfig)
		out.Code = "BRIDGE_UNKNOWN_PROVIDER"
	default:
		if out.Category == "" {
			out.Category = string(bridge.ErrInternal)
		}
		if out.Code == "" {
			out.Code = "BRIDGE_INTERNAL_ERROR"
		}
	}
	return out
}

func codeForCategory(cat bridge.ErrorCategory) string {
	switch cat {
	case bridge.ErrConfig:
		return "BRIDGE_CONFIG_ERROR"
	case bridge.ErrConnection:
		return "BRIDGE_CONNECTION_ERROR"
	case bridge.ErrSchema:
		return "BRIDGE_SCHEMA_ERROR"
	case bridge.ErrScan:
		return "BRIDGE_SCAN_ERROR"
	case bridge.ErrTransform:
		return "BRIDGE_TRANSFORM_ERROR"
	case bridge.ErrWrite:
		return "BRIDGE_WRITE_ERROR"
	case bridge.ErrVerify:
		return "BRIDGE_VERIFY_ERROR"
	case bridge.ErrCancelled:
		return "BRIDGE_CANCELLED"
	default:
		return "BRIDGE_INTERNAL_ERROR"
	}
}
