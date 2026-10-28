//go:build windows

package worker

import "fmt"

func reexec(path string) error {
	return fmt.Errorf("re-exec is not supported on Windows")
}
