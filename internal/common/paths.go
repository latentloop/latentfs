package common

import (
	"path/filepath"
	"strings"
)

// NormalizePath cleans and normalizes a path, removing leading/trailing slashes
func NormalizePath(path string) string {
	path = filepath.Clean(path)
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	if path == "." {
		return ""
	}
	return path
}

// SplitPath splits a path into its components
func SplitPath(path string) []string {
	path = NormalizePath(path)
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// JoinPath joins path components
func JoinPath(parts ...string) string {
	return NormalizePath(filepath.Join(parts...))
}

// ParentPath returns the parent directory of a path
func ParentPath(path string) string {
	path = NormalizePath(path)
	if path == "" {
		return ""
	}
	dir := filepath.Dir(path)
	if dir == "." {
		return ""
	}
	return dir
}

// BaseName returns the base name of a path
func BaseName(path string) string {
	path = NormalizePath(path)
	if path == "" {
		return ""
	}
	return filepath.Base(path)
}
