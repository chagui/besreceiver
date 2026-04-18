package besreceiver

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxWorkspaceItems       = 30
	maxBuildMetadataEntries = 20
	maxAttrValueLen         = 256
)

// sanitizeAttrKey normalizes a user-supplied key so it is safe to use as an
// OTel attribute suffix. Rules: lowercase, collapse whitespace to single '_',
// drop characters outside [a-z0-9_.], trim leading/trailing '_' and '.'.
// Returns "" for keys that reduce to empty; callers should skip those.
func sanitizeAttrKey(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	prevUnderscore := false
	for _, r := range s {
		switch {
		case unicode.IsSpace(r):
			if !prevUnderscore {
				b.WriteByte('_')
				prevUnderscore = true
			}
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.':
			b.WriteRune(r)
			prevUnderscore = r == '_'
		default:
			// drop
		}
	}
	return strings.Trim(b.String(), "_.")
}

// truncateAttrValue returns v bounded to maxAttrValueLen bytes, backing off to
// the nearest rune boundary so we never emit an invalid UTF-8 attribute.
func truncateAttrValue(v string) string {
	if len(v) <= maxAttrValueLen {
		return v
	}
	end := maxAttrValueLen
	for end > 0 && !utf8.RuneStart(v[end]) {
		end--
	}
	return v[:end]
}
