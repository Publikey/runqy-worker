package worker

import (
	"log"
	"os"
)

// Logger interface for worker logging.
type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
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

func (l *StdLogger) Debug(args ...interface{}) {
	l.logger.Print(append([]interface{}{"[DEBUG] "}, args...)...)
}

func (l *StdLogger) Info(args ...interface{}) {
	l.logger.Print(append([]interface{}{"[INFO] "}, args...)...)
}

func (l *StdLogger) Warn(args ...interface{}) {
	l.logger.Print(append([]interface{}{"[WARN] "}, args...)...)
}

func (l *StdLogger) Error(args ...interface{}) {
	l.logger.Print(append([]interface{}{"[ERROR] "}, args...)...)
}

func (l *StdLogger) Fatal(args ...interface{}) {
	l.logger.Fatal(append([]interface{}{"[FATAL] "}, args...)...)
}
