//go:build !windows

package bootstrap

import (
	"os"
	"syscall"
)

// sendTermSignal sends SIGTERM to the process for graceful shutdown.
func sendTermSignal(process *os.Process) error {
	return process.Signal(syscall.SIGTERM)
}
