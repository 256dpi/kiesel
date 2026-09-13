package kiesel

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	var lines []string
	logger := Logger(func(level Level, format string, args ...interface{}) {
		lines = append(lines, level.String()+": "+fmt.Sprintf(format, args...))
	})

	// Fatalf is left out, it exits the process
	logger.Infof("opened %q", "db")
	logger.Errorf("failed after %d tries", 3)

	assert.Equal(t, []string{
		`info: opened "db"`,
		"error: failed after 3 tries",
	}, lines)

	assert.Equal(t, "fatal", LevelFatal.String())
	assert.Equal(t, "unknown", Level(7).String())
}
