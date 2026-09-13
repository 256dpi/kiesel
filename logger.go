package kiesel

import (
	"os"

	"github.com/cockroachdb/pebble/v2"
)

// Level is the severity a message has been logged at.
type Level int

// The levels pebble logs at.
const (
	LevelInfo Level = iota
	LevelError
	LevelFatal
)

// String implements the fmt.Stringer interface.
func (l Level) String() string {
	switch l {
	case LevelInfo:
		return "info"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// Logger implements the pebble.Logger interface as a function. A call at
// LevelFatal exits the process, as pebble requires Fatalf to not return.
type Logger func(level Level, format string, args ...interface{})

var _ pebble.Logger = Logger(nil)

// Infof implements the pebble.Logger interface.
func (l Logger) Infof(format string, args ...interface{}) {
	l(LevelInfo, format, args...)
}

// Errorf implements the pebble.Logger interface.
func (l Logger) Errorf(format string, args ...interface{}) {
	l(LevelError, format, args...)
}

// Fatalf implements the pebble.Logger interface.
func (l Logger) Fatalf(format string, args ...interface{}) {
	l(LevelFatal, format, args...)
	os.Exit(1)
}
