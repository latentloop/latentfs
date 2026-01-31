package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Empty and root
		{"empty", "", ""},
		{"root", "/", ""},
		{"double_root", "//", ""},
		{"dot", ".", ""},

		// Simple paths
		{"simple", "foo", "foo"},
		{"leading_slash", "/foo", "foo"},
		{"trailing_slash", "foo/", "foo"},
		{"both_slashes", "/foo/", "foo"},

		// Nested paths
		{"two_parts", "foo/bar", "foo/bar"},
		{"two_parts_leading_slash", "/foo/bar", "foo/bar"},
		{"two_parts_trailing_slash", "/foo/bar/", "foo/bar"},
		{"three_parts", "foo/bar/baz", "foo/bar/baz"},

		// Paths with dots
		{"dot_prefix", "./foo", "foo"},
		{"dot_suffix", "foo/.", "foo"},
		{"dot_middle", "foo/./bar", "foo/bar"},
		{"dotdot_middle", "foo/../bar", "bar"},
		{"dotdot_middle_leading_slash", "/foo/../bar", "bar"},

		// Multiple slashes
		{"double_slash", "foo//bar", "foo/bar"},
		{"multiple_slashes", "/foo//bar//", "foo/bar"},
		{"many_slashes", "///foo///bar///", "foo/bar"},

		// Special cases
		{"dotdot", "..", ".."},
		{"dotdot_prefix", "../foo", "../foo"},
		{"dotdot_suffix", "foo/..", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizePath(tt.input)
			assert.Equal(t, tt.want, got, "NormalizePath(%q)", tt.input)
		})
	}
}

func TestSplitPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		// Empty and root
		{"empty", "", nil},
		{"root", "/", nil},
		{"dot", ".", nil},

		// Single component
		{"simple", "foo", []string{"foo"}},
		{"leading_slash", "/foo", []string{"foo"}},
		{"trailing_slash", "foo/", []string{"foo"}},

		// Multiple components
		{"two_parts", "foo/bar", []string{"foo", "bar"}},
		{"two_parts_leading_slash", "/foo/bar", []string{"foo", "bar"}},
		{"three_parts", "foo/bar/baz", []string{"foo", "bar", "baz"}},
		{"three_parts_both_slashes", "/foo/bar/baz/", []string{"foo", "bar", "baz"}},

		// With dots
		{"dot_prefix", "./foo", []string{"foo"}},
		{"dot_middle", "foo/./bar", []string{"foo", "bar"}},

		// Multiple slashes
		{"double_slash", "foo//bar", []string{"foo", "bar"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := SplitPath(tt.input)
			assert.Equal(t, tt.want, got, "SplitPath(%q)", tt.input)
		})
	}
}

func TestJoinPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		// Empty
		{"nil", nil, ""},
		{"empty_slice", []string{}, ""},
		{"empty_string", []string{""}, ""},

		// Single part
		{"single", []string{"foo"}, "foo"},
		{"single_leading_slash", []string{"/foo"}, "foo"},

		// Multiple parts
		{"two_parts", []string{"foo", "bar"}, "foo/bar"},
		{"three_parts", []string{"foo", "bar", "baz"}, "foo/bar/baz"},
		{"first_leading_slash", []string{"/foo", "bar"}, "foo/bar"},
		{"slashes_between", []string{"foo/", "/bar"}, "foo/bar"},

		// With empty parts
		{"empty_middle", []string{"foo", "", "bar"}, "foo/bar"},
		{"empty_first", []string{"", "foo", "bar"}, "foo/bar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := JoinPath(tt.parts...)
			assert.Equal(t, tt.want, got, "JoinPath(%v)", tt.parts)
		})
	}
}

func TestParentPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Empty and root
		{"empty", "", ""},
		{"root", "/", ""},
		{"dot", ".", ""},

		// Single component
		{"simple", "foo", ""},
		{"leading_slash", "/foo", ""},
		{"trailing_slash", "foo/", ""},

		// Nested paths
		{"two_parts", "foo/bar", "foo"},
		{"two_parts_leading_slash", "/foo/bar", "foo"},
		{"three_parts", "foo/bar/baz", "foo/bar"},
		{"three_parts_both_slashes", "/foo/bar/baz/", "foo/bar"},

		// With dots
		{"dot_prefix", "./foo", ""},
		{"dot_middle", "foo/./bar", "foo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ParentPath(tt.input)
			assert.Equal(t, tt.want, got, "ParentPath(%q)", tt.input)
		})
	}
}

func TestBaseName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Empty and root
		{"empty", "", ""},
		{"root", "/", ""},
		{"dot", ".", ""},

		// Single component
		{"simple", "foo", "foo"},
		{"leading_slash", "/foo", "foo"},
		{"trailing_slash", "foo/", "foo"},

		// Nested paths
		{"two_parts", "foo/bar", "bar"},
		{"two_parts_leading_slash", "/foo/bar", "bar"},
		{"three_parts", "foo/bar/baz", "baz"},
		{"three_parts_both_slashes", "/foo/bar/baz/", "baz"},

		// With extension
		{"with_ext", "foo.txt", "foo.txt"},
		{"nested_with_ext", "foo/bar.txt", "bar.txt"},
		{"full_path_with_ext", "/path/to/file.ext", "file.ext"},

		// With dots in path
		{"dot_prefix", "./foo", "foo"},
		{"dot_middle", "foo/./bar", "bar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BaseName(tt.input)
			assert.Equal(t, tt.want, got, "BaseName(%q)", tt.input)
		})
	}
}

func TestPathRoundtrip(t *testing.T) {
	t.Parallel()

	paths := []string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"a/b/c/d/e",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			parts := SplitPath(path)
			rejoined := JoinPath(parts...)
			assert.Equal(t, path, rejoined, "roundtrip failed")
		})
	}
}

func TestParentAndBaseName(t *testing.T) {
	t.Parallel()

	// For non-root paths, parent + base should give back the original
	paths := []string{
		"foo/bar",
		"a/b/c",
		"/path/to/file",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			parent := ParentPath(path)
			base := BaseName(path)
			if parent != "" {
				rejoined := JoinPath(parent, base)
				normalized := NormalizePath(path)
				assert.Equal(t, normalized, rejoined,
					"parent(%q)=%q, base(%q)=%q should rejoin correctly", path, parent, path, base)
			}
		})
	}
}
