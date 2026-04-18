package besreceiver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeAttrKey(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"BUILD_SCM_REVISION", "build_scm_revision"},
		{"STABLE_GIT_BRANCH", "stable_git_branch"},
		{"My Key", "my_key"},
		{"CI Run #42", "ci_run_42"},
		{"weird!@#key", "weirdkey"},
		{"already_lower.case", "already_lower.case"},
		{"___", ""},
		{"", ""},
		{"  spaces  ", "spaces"},
		{"multiple   spaces", "multiple_spaces"},
		{".leading.dot", "leading.dot"},
		{"trailing.", "trailing"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeAttrKey(tc.in))
		})
	}
}

func TestTruncateAttrValue(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, "", truncateAttrValue(""))
	})
	t.Run("under limit", func(t *testing.T) {
		v := strings.Repeat("a", 100)
		assert.Equal(t, v, truncateAttrValue(v))
	})
	t.Run("exactly at limit", func(t *testing.T) {
		v := strings.Repeat("a", maxAttrValueLen)
		assert.Equal(t, v, truncateAttrValue(v))
	})
	t.Run("over limit truncates", func(t *testing.T) {
		v := strings.Repeat("a", maxAttrValueLen+50)
		got := truncateAttrValue(v)
		assert.Len(t, got, maxAttrValueLen)
	})
	t.Run("multi-byte rune boundary", func(t *testing.T) {
		// Build a string where the 256th byte falls inside a multi-byte rune.
		// 'é' is 2 bytes in UTF-8. Place it so it straddles the cutoff.
		prefix := strings.Repeat("a", maxAttrValueLen-1)
		v := prefix + "é" + "tail"
		got := truncateAttrValue(v)
		assert.Len(t, got, maxAttrValueLen-1, "should back off to rune boundary")
		assert.Equal(t, prefix, got)
	})
}
