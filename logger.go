package worker

import (
	"fmt"
	"log"
	"os"
)

// Logger interface for worker logging.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Fatal(format string, args ...interface{})
}

// StdLogger is a simple logger using the standard library.
type StdLogger struct {
	logger *log.Logger
}

// NewStdLogger creates a new standard logger.
func NewStdLogger() *StdLogger {
	return &StdLogger{
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

func (l *StdLogger) Debug(format string, args ...interface{}) {
	l.logger.Print("[DEBUG] " + fmt.Sprintf(format, args...))
}

func (l *StdLogger) Info(format string, args ...interface{}) {
	l.logger.Print("[INFO] " + fmt.Sprintf(format, args...))
}

func (l *StdLogger) Warn(format string, args ...interface{}) {
	l.logger.Print("[WARN] " + fmt.Sprintf(format, args...))
}

func (l *StdLogger) Error(format string, args ...interface{}) {
	l.logger.Print("[ERROR] " + fmt.Sprintf(format, args...))
}

func (l *StdLogger) Fatal(format string, args ...interface{}) {
	l.logger.Fatal("[FATAL] " + fmt.Sprintf(format, args...))
}
