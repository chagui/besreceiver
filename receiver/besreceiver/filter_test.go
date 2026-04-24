package besreceiver

import (
	"strings"
	"testing"
)

func TestFilter_NilAndEmptyDefaultsToVerbose(t *testing.T) {
	var f *Filter
	if got := f.LevelForTarget("//any:label"); got != DetailLevelVerbose {
		t.Errorf("nil filter: expected verbose, got %q", got)
	}

	// Empty FilterConfig (as produced by no YAML block) must preserve
	// pre-filter behaviour.
	f = NewFilter(FilterConfig{})
	if got := f.LevelForTarget("//any:label"); got != DetailLevelVerbose {
		t.Errorf("empty FilterConfig: expected verbose, got %q", got)
	}
}

func TestFilter_DefaultLevelAppliesWhenNoRuleMatches(t *testing.T) {
	f := NewFilter(FilterConfig{DefaultLevel: DetailLevelTargets})
	if got := f.LevelForTarget("//unmatched:label"); got != DetailLevelTargets {
		t.Errorf("expected default=targets, got %q", got)
	}
}

func TestFilter_ExactPatternMatching(t *testing.T) {
	f := NewFilter(FilterConfig{
		DefaultLevel: DetailLevelVerbose,
		Rules: []FilterRule{
			{Pattern: "//pkg:target", DetailLevel: DetailLevelDrop},
		},
	})

	cases := map[string]DetailLevel{
		"//pkg:target":       DetailLevelDrop,    // exact match
		"//pkg:target_other": DetailLevelVerbose, // different target
		"//pkg2:target":      DetailLevelVerbose, // different package
		"//pkg/sub:target":   DetailLevelVerbose, // subpath is not covered by exact match
	}
	for label, want := range cases {
		if got := f.LevelForTarget(label); got != want {
			t.Errorf("LevelForTarget(%q) = %q, want %q", label, got, want)
		}
	}
}

func TestFilter_PackageWildcardMatching(t *testing.T) {
	// //pkg:* matches every target in //pkg but not sub-packages.
	f := NewFilter(FilterConfig{
		DefaultLevel: DetailLevelVerbose,
		Rules: []FilterRule{
			{Pattern: "//pkg:*", DetailLevel: DetailLevelDrop},
		},
	})

	cases := map[string]DetailLevel{
		"//pkg:lib":      DetailLevelDrop,
		"//pkg:test":     DetailLevelDrop,
		"//pkg/sub:lib":  DetailLevelVerbose, // sub-package — not matched by :*
		"//pkgabc:lib":   DetailLevelVerbose, // prefix collision guarded by ':' anchor
		"//otherpkg:lib": DetailLevelVerbose,
	}
	for label, want := range cases {
		if got := f.LevelForTarget(label); got != want {
			t.Errorf("LevelForTarget(%q) = %q, want %q", label, got, want)
		}
	}
}

func TestFilter_SubtreePatternMatching(t *testing.T) {
	// //pkg/... matches //pkg:target AND any //pkg/sub[/...]:target, but not
	// packages that merely share a prefix (//pkgabc:...).
	f := NewFilter(FilterConfig{
		DefaultLevel: DetailLevelVerbose,
		Rules: []FilterRule{
			{Pattern: "//pkg/...", DetailLevel: DetailLevelDrop},
		},
	})

	cases := map[string]DetailLevel{
		"//pkg:target":          DetailLevelDrop,
		"//pkg/sub:target":      DetailLevelDrop,
		"//pkg/sub/more:target": DetailLevelDrop,
		"//pkgabc:target":       DetailLevelVerbose, // prefix-only collision
		"//pkgabc/sub:target":   DetailLevelVerbose,
		"//other:target":        DetailLevelVerbose,
	}
	for label, want := range cases {
		if got := f.LevelForTarget(label); got != want {
			t.Errorf("LevelForTarget(%q) = %q, want %q", label, got, want)
		}
	}
}

func TestFilter_FirstMatchWins(t *testing.T) {
	// Rule order matters: the //pkg:lib exact match comes before the
	// //pkg:* package sweep, so //pkg:lib gets verbose while its neighbours
	// get dropped.
	f := NewFilter(FilterConfig{
		DefaultLevel: DetailLevelDrop,
		Rules: []FilterRule{
			{Pattern: "//pkg:lib", DetailLevel: DetailLevelVerbose},
			{Pattern: "//pkg:*", DetailLevel: DetailLevelDrop},
		},
	})

	if got := f.LevelForTarget("//pkg:lib"); got != DetailLevelVerbose {
		t.Errorf("//pkg:lib should win first rule: got %q, want verbose", got)
	}
	if got := f.LevelForTarget("//pkg:other"); got != DetailLevelDrop {
		t.Errorf("//pkg:other should fall through to second rule: got %q, want drop", got)
	}
	if got := f.LevelForTarget("//outside:x"); got != DetailLevelDrop {
		t.Errorf("//outside:x falls through to default: got %q, want drop", got)
	}
}

func TestFilter_FirstMatchWins_ReversedOrder(t *testing.T) {
	// Swap the order: now //pkg:* claims every target first, so the more
	// specific //pkg:lib rule is unreachable — a lint a human author would
	// flag, but Filter itself must honour the declared order.
	f := NewFilter(FilterConfig{
		DefaultLevel: DetailLevelDrop,
		Rules: []FilterRule{
			{Pattern: "//pkg:*", DetailLevel: DetailLevelDrop},
			{Pattern: "//pkg:lib", DetailLevel: DetailLevelVerbose},
		},
	})
	if got := f.LevelForTarget("//pkg:lib"); got != DetailLevelDrop {
		t.Errorf("first-match-wins: //pkg:lib should hit :* rule, got %q", got)
	}
}

func TestFilter_EmptyLabelUsesDefault(t *testing.T) {
	f := NewFilter(FilterConfig{
		DefaultLevel: DetailLevelTargets,
		Rules: []FilterRule{
			{Pattern: "//pkg/...", DetailLevel: DetailLevelDrop},
		},
	})
	if got := f.LevelForTarget(""); got != DetailLevelTargets {
		t.Errorf("empty label should fall through to default, got %q", got)
	}
}

func TestFilterConfigValidate_AcceptsEmpty(t *testing.T) {
	// No filter block: empty config validates.
	fc := FilterConfig{}
	if err := fc.Validate(); err != nil {
		t.Errorf("empty FilterConfig should validate, got: %v", err)
	}
}

func TestFilterConfigValidate_AcceptsAllKnownLevels(t *testing.T) {
	for _, level := range []DetailLevel{DetailLevelDrop, DetailLevelBuildOnly, DetailLevelTargets, DetailLevelVerbose} {
		fc := FilterConfig{
			DefaultLevel: level,
			Rules: []FilterRule{
				{Pattern: "//pkg:*", DetailLevel: level},
			},
		}
		if err := fc.Validate(); err != nil {
			t.Errorf("level %q should validate, got: %v", level, err)
		}
	}
}

func TestFilterConfigValidate_RejectsUnknownDefaultLevel(t *testing.T) {
	fc := FilterConfig{DefaultLevel: "summary"}
	err := fc.Validate()
	if err == nil {
		t.Fatal("expected error for unknown default_level")
	}
	if !strings.Contains(err.Error(), "default_level") {
		t.Errorf("error should mention default_level, got: %v", err)
	}
}

func TestFilterConfigValidate_RejectsUnknownRuleLevel(t *testing.T) {
	fc := FilterConfig{
		Rules: []FilterRule{
			{Pattern: "//pkg:*", DetailLevel: "loud"},
		},
	}
	err := fc.Validate()
	if err == nil {
		t.Fatal("expected error for unknown rule detail_level")
	}
	if !strings.Contains(err.Error(), "rules[0]") {
		t.Errorf("error should mention the failing rule index, got: %v", err)
	}
}

func TestFilterConfigValidate_RejectsEmptyPattern(t *testing.T) {
	fc := FilterConfig{
		Rules: []FilterRule{
			{Pattern: "", DetailLevel: DetailLevelDrop},
		},
	}
	if err := fc.Validate(); err == nil {
		t.Fatal("expected error for empty pattern")
	}
}

func TestFilterConfigValidate_RejectsMalformedPatterns(t *testing.T) {
	cases := []string{
		"pkg:target",   // missing //
		"@repo//pkg:t", // external repos not supported
		"//:target",    // empty package
		"//pkg",        // no : and no /...
		"///...",       // empty subtree prefix
	}
	for _, pattern := range cases {
		fc := FilterConfig{
			Rules: []FilterRule{
				{Pattern: pattern, DetailLevel: DetailLevelDrop},
			},
		}
		if err := fc.Validate(); err == nil {
			t.Errorf("pattern %q should have failed validation", pattern)
		}
	}
}

func TestDetailLevel_AllowFlags(t *testing.T) {
	cases := map[DetailLevel]struct {
		target bool
		action bool
	}{
		DetailLevelDrop:      {target: false, action: false},
		DetailLevelBuildOnly: {target: false, action: false},
		DetailLevelTargets:   {target: true, action: false},
		DetailLevelVerbose:   {target: true, action: true},
	}
	for level, want := range cases {
		if got := level.allowTarget(); got != want.target {
			t.Errorf("%q.allowTarget() = %v, want %v", level, got, want.target)
		}
		if got := level.allowAction(); got != want.action {
			t.Errorf("%q.allowAction() = %v, want %v", level, got, want.action)
		}
	}
}
