package kiesel

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func withDB(noSync bool, fn func(db *pebble.DB, fs vfs.FS)) {
	fs := vfs.NewMem()

	db, err := pebble.Open("", &pebble.Options{
		FS:         fs,
		DisableWAL: noSync,
	})
	if err != nil {
		panic(err)
	}

	fn(db, fs)

	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func scan(db ReadWriter) map[string]string {
	ret := map[string]string{}

	iter, err := db.NewIter(nil)
	if err != nil {
		panic(err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		ret[string(iter.Key())] = string(iter.Value())
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}

	return ret
}
