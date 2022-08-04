package kiesel

import (
	_ "unsafe" // allow links

	"github.com/cockroachdb/pebble"
)

// NewCacheWithShards allows the creation of a cache with an explicit number of
// shards.
//go:linkname NewCacheWithShards github.com/cockroachdb/pebble/internal/cache.newShards
//go:nosplit
func NewCacheWithShards(size int64, shards int) *pebble.Cache
