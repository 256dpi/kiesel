package kiesel

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
)

var optionsString = `[Version]
  pebble_version=0.1

[Options]
  bytes_per_sync=524288
  cache_size=1024
  cleaner=delete
  compaction_debt_concurrency=1073741824
  comparer=leveldb.BytewiseComparator
  disable_wal=false
  flush_delay_delete_range=10s
  flush_delay_range_key=10s
  flush_split_bytes=4194304
  format_major_version=1
  l0_compaction_concurrency=10
  l0_compaction_file_threshold=500
  l0_compaction_threshold=2
  l0_stop_writes_threshold=1000
  lbase_max_bytes=67108864
  max_concurrent_compactions=3
  max_manifest_file_size=134217728
  max_open_files=1000
  mem_table_size=67108864
  mem_table_stop_writes_threshold=4
  min_deletion_rate=134217728
  merger=pebble.concatenate
  read_compaction_rate=16000
  read_sampling_multiplier=16
  strict_wal_tail=true
  table_cache_shards=2
  table_property_collectors=[]
  validate_on_ingest=false
  wal_dir=
  wal_bytes_per_sync=0
  max_writer_concurrency=0
  force_writer_parallelism=false

[Level "0"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=rocksdb.BuiltinBloomFilter
  filter_type=table
  index_block_size=262144
  target_file_size=2097152

[Level "1"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=rocksdb.BuiltinBloomFilter
  filter_type=table
  index_block_size=262144
  target_file_size=4194304

[Level "2"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=rocksdb.BuiltinBloomFilter
  filter_type=table
  index_block_size=262144
  target_file_size=8388608

[Level "3"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=rocksdb.BuiltinBloomFilter
  filter_type=table
  index_block_size=262144
  target_file_size=16777216

[Level "4"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=rocksdb.BuiltinBloomFilter
  filter_type=table
  index_block_size=262144
  target_file_size=33554432

[Level "5"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=rocksdb.BuiltinBloomFilter
  filter_type=table
  index_block_size=262144
  target_file_size=67108864

[Level "6"]
  block_restart_interval=16
  block_size=32768
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=262144
  target_file_size=134217728
`

func TestCockroachOptions(t *testing.T) {
	fs := vfs.NewMem()
	cache := pebble.NewCache(1024)

	opts := BuildOptions(Blueprint{
		FS:    fs,
		Cache: cache,
	})
	opts.Experimental.TableCacheShards = 2
	assert.Equal(t, optionsString, opts.String())
}
