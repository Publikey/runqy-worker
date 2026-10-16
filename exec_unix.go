//go:build !windows

package worker

import (
	"os"
	"syscall"
)

// reexec replaces the current process with the binary at path, keeping the arguments
// and environment (same PID, so the worker id is unchanged).
func reexec(path string) error {
	return syscall.Exec(path, os.Args, os.Environ())
}
