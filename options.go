package kiesel

import (
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

// Blueprint describes the values used to build options for pebble.
type Blueprint struct {
	// The file system implementation.
	FS vfs.FS

	// The block cache instance.
	Cache *pebble.Cache

	// Whether to not use bloom filters on all except the last level.
	NoFilters bool

	// The number of levels to use.
	//
	// Default: 7
	Levels int

	// The mem table size.
	//
	// Default: 64 MiB
	MemTableSize int

	// The maximum number of active mem tables.
	//
	// Default: 4
	MaxMemTables int

	// The maximum size of the base level.
	//
	// Pebble uses an algorithm to dynamically calculate the level sizes similar
	// to the one found in RocksDB. This options forces a cap on the level L0
	// is compacted into to ensure new levels are created when needed.
	// https://github.com/cockroachdb/pebble/blob/master/options.go#L538
	// https://github.com/cockroachdb/pebble/commit/22071b0f2df784875d3be05132f71396463b0dc4
	// https://github.com/cockroachdb/pebble/commit/f5d25937abe21677033114db971e3acf863bc1a3
	// http://rocksdb.org/blog/2015/07/23/dynamic-level.html
	//
	// Default: 64 MiB
	MaxBaseLevelSize int

	// The start file size for the first level and the file size multiplier for
	// the following levels.
	//
	// Default: 2 MiB, 2x
	StartFileSize      int
	FileSizeMultiplier int

	// The data and index block size.
	//
	// Default: 32 KiB, 256 KiB
	DataBlockSize  int
	IndexBlockSize int
}

func (b *Blueprint) ensureDefaults() {
	if b.Levels <= 0 {
		b.Levels = 7
	}
	if b.MemTableSize <= 0 {
		b.MemTableSize = 64 << 20
	}
	if b.MaxMemTables <= 0 {
		b.MaxMemTables = 4
	}
	if b.MaxBaseLevelSize <= 0 {
		b.MaxBaseLevelSize = 64 << 20
	}
	if b.StartFileSize <= 0 {
		b.StartFileSize = 2 << 20
	}
	if b.FileSizeMultiplier <= 0 {
		b.FileSizeMultiplier = 2
	}
	if b.DataBlockSize <= 0 {
		b.DataBlockSize = 32 << 10
	}
	if b.IndexBlockSize <= 0 {
		b.IndexBlockSize = 256 << 10
	}
}

// BuildOptions supports building pebble options based on a blueprint. The
// default values applied to the blueprint represents the chosen settings in
// CockroachDB the primary users of pebble.
// https://github.com/cockroachdb/cockroach/blob/master/pkg/storage/pebble.go#L488
// https://github.com/cockroachdb/pebble/blob/master/cmd/pebble/db.go#L53
func BuildOptions(bp Blueprint) *pebble.Options {
	// ensure defaults
	bp.ensureDefaults()

	// prepare options
	opts := &pebble.Options{
		FS:                          bp.FS,
		Cache:                       bp.Cache,
		FormatMajorVersion:          pebble.FormatDefault,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               int64(bp.MaxBaseLevelSize),
		Levels:                      make([]pebble.LevelOptions, bp.Levels),
		MaxConcurrentCompactions:    func() int { return 3 },
		MaxOpenFiles:                1000,
		MemTableSize:                bp.MemTableSize,
		MemTableStopWritesThreshold: bp.MaxMemTables,
	}

	// configure levels
	for i := 0; i < len(opts.Levels); i++ {
		// get level
		l := &opts.Levels[i]

		// set basics
		l.BlockSize = bp.DataBlockSize
		l.IndexBlockSize = bp.IndexBlockSize

		// use filter if requested
		if !bp.NoFilters {
			l.FilterPolicy = bloom.FilterPolicy(10)
			l.FilterType = pebble.TableFilter
		}

		// set or multiply target file size for each level
		if i == 0 {
			l.TargetFileSize = int64(bp.StartFileSize)
		} else {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * int64(bp.FileSizeMultiplier)
		}

		// ensure defaults
		l.EnsureDefaults()
	}
	if !bp.NoFilters {
		// disable filter on last level
		opts.Levels[6].FilterPolicy = nil
	}

	// ensure database is flushed when ranges are deleted
	opts.Experimental.DeleteRangeFlushDelay = 10 * time.Second

	// pace deletions to a reasonable amount
	opts.Experimental.MinDeletionRate = 128 << 20 // 128 MB

	// ensure defaults
	opts.EnsureDefaults()

	return opts
}
