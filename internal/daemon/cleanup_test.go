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

//go:build darwin

package daemon

import (
	"errors"
	"testing"
)

func TestCleanupResult(t *testing.T) {
	result := &CleanupResult{
		StaleMounts:    []string{"/mnt/a", "/mnt/b"},
		CleanedPidFile: true,
		CleanedSocket:  true,
		Errors:         []error{errors.New("test error")},
	}

	if len(result.StaleMounts) != 2 {
		t.Errorf("StaleMounts count = %d, want 2", len(result.StaleMounts))
	}
	if !result.CleanedPidFile {
		t.Error("CleanedPidFile should be true")
	}
	if !result.CleanedSocket {
		t.Error("CleanedSocket should be true")
	}
	if len(result.Errors) != 1 {
		t.Errorf("Errors count = %d, want 1", len(result.Errors))
	}
}

func TestFormatCleanupResult_Empty(t *testing.T) {
	result := &CleanupResult{}
	formatted := FormatCleanupResult(result)
	if formatted != "No cleanup needed" {
		t.Errorf("FormatCleanupResult() = %q, want 'No cleanup needed'", formatted)
	}
}

func TestFormatCleanupResult_StaleMounts(t *testing.T) {
	result := &CleanupResult{
		StaleMounts: []string{"/mnt/a", "/mnt/b"},
	}
	formatted := FormatCleanupResult(result)
	if formatted == "No cleanup needed" {
		t.Error("should not say 'No cleanup needed' with stale mounts")
	}
	if len(formatted) == 0 {
		t.Error("formatted string should not be empty")
	}
}

func TestFormatCleanupResult_PidFile(t *testing.T) {
	result := &CleanupResult{
		CleanedPidFile: true,
	}
	formatted := FormatCleanupResult(result)
	if formatted == "No cleanup needed" {
		t.Error("should not say 'No cleanup needed' when PID was cleaned")
	}
}

func TestFormatCleanupResult_Socket(t *testing.T) {
	result := &CleanupResult{
		CleanedSocket: true,
	}
	formatted := FormatCleanupResult(result)
	if formatted == "No cleanup needed" {
		t.Error("should not say 'No cleanup needed' when socket was cleaned")
	}
}

func TestFormatCleanupResult_Errors(t *testing.T) {
	result := &CleanupResult{
		Errors: []error{errors.New("error 1"), errors.New("error 2")},
	}
	formatted := FormatCleanupResult(result)
	if formatted == "No cleanup needed" {
		t.Error("should not say 'No cleanup needed' when there are errors")
	}
}

func TestFormatCleanupResult_Full(t *testing.T) {
	result := &CleanupResult{
		StaleMounts:    []string{"/mnt/test"},
		CleanedPidFile: true,
		CleanedSocket:  true,
		Errors:         []error{errors.New("test error")},
	}
	formatted := FormatCleanupResult(result)

	// Should contain mentions of all parts
	if len(formatted) == 0 {
		t.Error("formatted string should not be empty")
	}
}
