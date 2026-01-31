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
