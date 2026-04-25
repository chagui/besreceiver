package besreceiver

import (
	"fmt"
	"strings"
)

// DetailLevel controls how many spans a target produces. Levels form a
// strict hierarchy from least to most verbose: drop < build_only < targets <
// verbose. The root bazel.build span and the bazel.metrics span are always
// emitted regardless of level — filtering only applies to per-target work.
type DetailLevel string

const (
	// DetailLevelDrop suppresses every per-target span (target / action /
	// test). Matched targets contribute nothing except via the always-on
	// root span and BuildMetrics.
	DetailLevelDrop DetailLevel = "drop"
	// DetailLevelBuildOnly emits only the root bazel.build span. Semantically
	// identical to DetailLevelDrop at the per-target level; kept as a
	// distinct name because the default_level knob applies to the whole
	// invocation, where "build_only" reads more naturally than "drop".
	DetailLevelBuildOnly DetailLevel = "build_only"
	// DetailLevelTargets emits bazel.target spans but suppresses child
	// bazel.action and bazel.test spans.
	DetailLevelTargets DetailLevel = "targets"
	// DetailLevelVerbose emits every span (target + action + test). Matches
	// the pre-filter behaviour of the receiver.
	DetailLevelVerbose DetailLevel = "verbose"
)

// validDetailLevels enumerates the accepted DetailLevel values for config
// validation. Kept as a set so Validate can produce a deterministic,
// sorted error message.
//
//nolint:gochecknoglobals // immutable lookup table; Go has no map const
var validDetailLevels = map[DetailLevel]struct{}{
	DetailLevelDrop:      {},
	DetailLevelBuildOnly: {},
	DetailLevelTargets:   {},
	DetailLevelVerbose:   {},
}

// allowTarget returns true if the level permits emission of bazel.target
// spans.
func (d DetailLevel) allowTarget() bool {
	switch d {
	case DetailLevelTargets, DetailLevelVerbose:
		return true
	case DetailLevelDrop, DetailLevelBuildOnly:
		return false
	}
	return false
}

// allowAction returns true if the level permits emission of bazel.action
// and bazel.test spans.
func (d DetailLevel) allowAction() bool {
	return d == DetailLevelVerbose
}

// allowFetch returns true if the level permits emission of bazel.fetch
// spans. Fetch spans are per-invocation detail with no owning target, so
// drop / build_only suppress them; targets and verbose keep them. The
// split from allowAction matters because an operator may want per-target
// spans suppressed (DetailLevelTargets or below for the action gate) yet
// still see fetch activity, which is frequently the dominant cost on
// clean builds.
func (d DetailLevel) allowFetch() bool {
	switch d {
	case DetailLevelTargets, DetailLevelVerbose:
		return true
	case DetailLevelDrop, DetailLevelBuildOnly:
		return false
	}
	return false
}

// FilterConfig configures per-target detail level filtering. When the
// surrounding receiver config omits this block entirely, DefaultLevel is
// empty and Rules is nil; NewFilter treats that as verbose (current
// behaviour unchanged).
type FilterConfig struct {
	// DefaultLevel applies when no rule matches. Empty string is normalised
	// to verbose at Filter construction time so an omitted block keeps
	// pre-existing behaviour.
	DefaultLevel DetailLevel `mapstructure:"default_level"`
	// Rules are evaluated in order; the first rule whose pattern matches
	// the target label wins. Later rules are not consulted.
	Rules []FilterRule `mapstructure:"rules"`
}

// FilterRule pairs a target label pattern with the detail level to apply
// when the pattern matches.
type FilterRule struct {
	Pattern     string      `mapstructure:"pattern"`
	DetailLevel DetailLevel `mapstructure:"detail_level"`
}

// Validate checks the FilterConfig for unknown detail levels and malformed
// patterns. An empty DefaultLevel is accepted — Filter will normalise it.
func (fc *FilterConfig) Validate() error {
	if fc.DefaultLevel != "" {
		if _, ok := validDetailLevels[fc.DefaultLevel]; !ok {
			return fmt.Errorf("filter.default_level: invalid detail level %q (allowed: drop, build_only, targets, verbose)", fc.DefaultLevel)
		}
	}
	for i, rule := range fc.Rules {
		if rule.Pattern == "" {
			return fmt.Errorf("filter.rules[%d]: pattern must not be empty", i)
		}
		if _, ok := validDetailLevels[rule.DetailLevel]; !ok {
			return fmt.Errorf("filter.rules[%d]: invalid detail level %q (allowed: drop, build_only, targets, verbose)", i, rule.DetailLevel)
		}
		if err := validatePattern(rule.Pattern); err != nil {
			return fmt.Errorf("filter.rules[%d]: %w", i, err)
		}
	}
	return nil
}

// patternKind enumerates the three supported Bazel label pattern shapes.
// Kept internal — callers interact with Filter, not with compiled patterns
// directly.
type patternKind int

const (
	patternExact   patternKind = iota // //pkg:target — exact match
	patternPkgAll                     // //pkg:*     — all targets in one package
	patternSubtree                    // //pkg/...   — package and all sub-packages
)

// compiledRule is a FilterRule with its pattern pre-parsed for fast matching.
type compiledRule struct {
	kind   patternKind
	prefix string // for patternPkgAll / patternSubtree; empty for exact
	exact  string // for patternExact; empty otherwise
	level  DetailLevel
}

func (r compiledRule) match(label string) bool {
	switch r.kind {
	case patternExact:
		return label == r.exact
	case patternPkgAll:
		// //pkg:*  → match anything in //pkg:, including //pkg: itself.
		return strings.HasPrefix(label, r.prefix)
	case patternSubtree:
		// //pkg/... → match //pkg:target, //pkg/sub:target, //pkg/sub/more:t.
		// The pkg itself is matched by either //pkg: or //pkg/ prefix.
		return strings.HasPrefix(label, r.prefix+":") || strings.HasPrefix(label, r.prefix+"/")
	}
	return false
}

// Filter applies FilterConfig rules to target labels. It is immutable after
// construction and safe for concurrent reads — which the single-goroutine
// owner of TraceBuilder doesn't need, but keeps the type trivially
// shareable for future multi-goroutine refactors.
type Filter struct {
	defaultLevel DetailLevel
	rules        []compiledRule
}

// NewFilter compiles a FilterConfig into a reusable Filter. Patterns are
// assumed valid; callers should have already run FilterConfig.Validate (the
// Collector does so via Config.Validate before Start).
func NewFilter(cfg FilterConfig) *Filter {
	level := cfg.DefaultLevel
	if level == "" {
		level = DetailLevelVerbose
	}
	rules := make([]compiledRule, 0, len(cfg.Rules))
	for _, r := range cfg.Rules {
		c, err := compilePattern(r.Pattern)
		if err != nil {
			// Validate() is expected to have caught this; skipping defensively.
			continue
		}
		c.level = r.DetailLevel
		rules = append(rules, c)
	}
	return &Filter{defaultLevel: level, rules: rules}
}

// LevelForTarget returns the detail level to apply to the given target
// label. An empty label is treated the same as an unmatched label — the
// default level applies. The first matching rule wins; later rules do not
// override.
func (f *Filter) LevelForTarget(label string) DetailLevel {
	if f == nil {
		return DetailLevelVerbose
	}
	for _, r := range f.rules {
		if r.match(label) {
			return r.level
		}
	}
	return f.defaultLevel
}

// validatePattern runs pattern compilation purely for its error result.
func validatePattern(pattern string) error {
	_, err := compilePattern(pattern)
	return err
}

// compilePattern parses a Bazel label pattern into a compiledRule. The
// accepted forms are:
//
//   - //pkg/...       — subtree match (package and all sub-packages)
//   - //pkg:*         — all targets in a single package
//   - //pkg:target    — exact match on one label
//
// External repo patterns (@repo//pkg:t) and relative patterns (pkg:t) are
// intentionally rejected: Bazel emits canonicalised absolute labels in BEP,
// so accepting looser forms would silently never match.
func compilePattern(pattern string) (compiledRule, error) {
	if !strings.HasPrefix(pattern, "//") {
		return compiledRule{}, fmt.Errorf("pattern %q: must start with // (external repos like @repo//... are not supported)", pattern)
	}

	// //pkg/... form: match a whole subtree.
	if prefix, ok := strings.CutSuffix(pattern, "/..."); ok {
		// Reject empty or single-slash packages: "//..." and "///..." provide
		// no useful filter (the first is an all-targets glob, the second is
		// malformed). We require at least //<non-empty-segment>/...
		if prefix == "//" || prefix == "///" || prefix == "/" || prefix == "" {
			return compiledRule{}, fmt.Errorf("pattern %q: package path before /... must not be empty", pattern)
		}
		return compiledRule{kind: patternSubtree, prefix: prefix}, nil
	}

	// //pkg:* form: match every target in a single package.
	if _, ok := strings.CutSuffix(pattern, ":*"); ok {
		prefix := strings.TrimSuffix(pattern, "*")
		// prefix now ends with ":" — strings.HasPrefix(label, prefix) requires
		// the same ':' anchor so "//pkga:*" does not match "//pkgabc:t".
		if prefix == "//:" {
			return compiledRule{}, fmt.Errorf("pattern %q: package path before :* must not be empty", pattern)
		}
		return compiledRule{kind: patternPkgAll, prefix: prefix}, nil
	}

	// //pkg:target form: exact label — requires ':' to separate package and target.
	if !strings.Contains(pattern, ":") {
		return compiledRule{}, fmt.Errorf("pattern %q: exact labels must include ':target' (did you mean %q/...?)", pattern, pattern)
	}
	// Reject //:target — empty package.
	if strings.HasPrefix(pattern, "//:") {
		return compiledRule{}, fmt.Errorf("pattern %q: package path must not be empty", pattern)
	}
	return compiledRule{kind: patternExact, exact: pattern}, nil
}
