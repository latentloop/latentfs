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

package commands

import (
	"context"

	"latentfs/internal/daemon"
	"latentfs/internal/util"
)

// StartDaemonIfNeeded starts the daemon in the background if not running.
// If notify is true, prints a message to inform the user.
// Returns nil if daemon is already running or successfully started.
func StartDaemonIfNeeded(notify bool) error {
	cfg := util.DaemonStartConfig{
		Notify:     notify,
		PollConfig: util.FastPollConfig(),
	}

	return util.StartDaemonIfNeeded(
		context.Background(),
		cfg,
		daemon.IsDaemonRunning,
		[]string{"daemon", "start"},
	)
}
