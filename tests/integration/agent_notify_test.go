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

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

func TestAgentNotify(t *testing.T) {
	t.Skip("agent command temporarily disabled")

	env := NewSharedDaemonTestEnv(t, "agent_notify")
	defer env.Cleanup()

	t.Run("notify succeeds with project config", func(t *testing.T) {
		g := NewWithT(t)

		// Create project dir with .latentfs/config.yaml
		projectDir := filepath.Join(env.TestDir, "project_with_config")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		err := os.MkdirAll(latentfsDir, 0755)
		g.Expect(err).NotTo(HaveOccurred())

		configContent := "autosave: true\nlogging: none\n"
		err = os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)
		g.Expect(err).NotTo(HaveOccurred())

		result := runAgentNotify(env.configDir, projectDir, "", "PRE_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify should succeed: %s", result.Combined)
	})

	t.Run("notify succeeds without project config", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_no_config")
		err := os.MkdirAll(projectDir, 0755)
		g.Expect(err).NotTo(HaveOccurred())

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify should succeed: %s", result.Combined)
	})

	t.Run("notify succeeds with empty project dir", func(t *testing.T) {
		g := NewWithT(t)

		result := runAgentNotify(env.configDir, "", "", "PRE_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify should succeed: %s", result.Combined)
	})

	t.Run("notify with stdin payload", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_stdin")
		err := os.MkdirAll(projectDir, 0755)
		g.Expect(err).NotTo(HaveOccurred())

		payload := `{"session_id":"s1","tool_name":"Bash","event_type":"PRE_TOOL_USE"}`
		result := runAgentNotify(env.configDir, projectDir, payload, "PRE_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify with stdin should succeed: %s", result.Combined)
	})

	t.Run("notify requires event type argument", func(t *testing.T) {
		g := NewWithT(t)

		result := env.RunCLI("agent", "notify")
		g.Expect(result.ExitCode).NotTo(Equal(0), "agent notify without event_type should fail")
	})

	t.Run("daemon logs agent event", func(t *testing.T) {
		g := NewWithT(t)

		// Create project dir
		projectDir := filepath.Join(env.TestDir, "project_autosave_log")
		err := os.MkdirAll(projectDir, 0755)
		g.Expect(err).NotTo(HaveOccurred())

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0))

		// Check daemon log for agent notify entry (daemon only logs, autosave runs in CLI)
		home, _ := os.UserHomeDir()
		logPath := filepath.Join(home, ".latentfs", env.configDir+".log")
		logData, err := os.ReadFile(logPath)
		g.Expect(err).NotTo(HaveOccurred(), "daemon log should exist (started with --logging)")

		logContent := string(logData)
		g.Expect(logContent).To(ContainSubstring("handleAgentNotify"), "daemon log should contain agent notify entries")
	})
}

// runAgentNotify runs `latentfs agent notify <eventType>` with CLAUDE_PROJECT_DIR and optional stdin.
func runAgentNotify(configDir, projectDir, stdinData, eventType string) CLIResult {
	ctx, cancel := context.WithTimeout(context.Background(), CLITimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cliBinary, "agent", "notify", eventType)
	cmd.WaitDelay = 2 * CLITimeout

	// Build env with LATENTFS_CONFIG_DIR and CLAUDE_PROJECT_DIR
	cmdEnv := filterEnvExcluding("LATENTFS_CONFIG_DIR")
	cmdEnv = filterSlice(cmdEnv, "CLAUDE_PROJECT_DIR")
	if configDir != "" {
		cmdEnv = append(cmdEnv, "LATENTFS_CONFIG_DIR="+configDir)
	}
	if projectDir != "" {
		cmdEnv = append(cmdEnv, "CLAUDE_PROJECT_DIR="+projectDir)
	}
	cmdEnv = append(cmdEnv, fmt.Sprintf("LATENTFS_PARENT_PID=%d", os.Getpid()))
	cmd.Env = cmdEnv

	if stdinData != "" {
		cmd.Stdin = strings.NewReader(stdinData)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	exitCode := 0
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			exitCode = 124
		} else if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
		}
	}

	combined := stdout.String() + stderr.String()
	return CLIResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Combined: combined,
		ExitCode: exitCode,
	}
}

// filterSlice removes env vars with the given prefix from a slice
func filterSlice(envSlice []string, exclude string) []string {
	prefix := exclude + "="
	result := make([]string, 0, len(envSlice))
	for _, e := range envSlice {
		if !strings.HasPrefix(e, prefix) {
			result = append(result, e)
		}
	}
	return result
}
