// Copyright 2024 LatentFS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
