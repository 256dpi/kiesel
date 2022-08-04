package kiesel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrefixRangeEnd(t *testing.T) {
	end := EndPrefixRange(nil)
	assert.Nil(t, end)

	end = EndPrefixRange([]byte("foo"))
	assert.Equal(t, "fop", string(end))

	end = EndPrefixRange([]byte{0xFF, 0x01})
	assert.Equal(t, []byte{0xFF, 0x02}, end)

	end = EndPrefixRange([]byte{0x01, 0xFF})
	assert.Equal(t, []byte{0x02}, end)

	end = EndPrefixRange([]byte{0xFF, 0xFF})
	assert.Nil(t, end)
}
