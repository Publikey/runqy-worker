//go:build windows

package bootstrap

import "os"

// sendTermSignal on Windows falls back to Kill() since SIGTERM is not supported.
func sendTermSignal(process *os.Process) error {
	return process.Kill()
}
