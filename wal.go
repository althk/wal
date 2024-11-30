// Package wal provides a simple interface for a write-ahead-log built on top of LevelDB
package wal

import (
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"iter"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// WAL provides a write-ahead-log built over LevelDB
type WAL struct {
	db *leveldb.DB
}

// New initializes a new WAL service with a LevelDB created at the given file path.
// It is an error if the file path already exists.
func New(path string) (*WAL, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &WAL{db: db}, nil
}

func (wal *WAL) Close() error {
	return wal.db.Close()
}

func (wal *WAL) Put(key string, value []byte) error {
	return wal.db.Put([]byte(key), value, nil)
}

func (wal *WAL) Get(key string) ([]byte, error) {
	v, err := wal.db.Get([]byte(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, ErrKeyNotFound
	}
	return v, err
}

func (wal *WAL) Delete(key string) error {
	return wal.db.Delete([]byte(key), nil)
}

// PutBatch writes multiple entries in one batch, which is more efficient than writing
// multiple entries one at a time.
func (wal *WAL) PutBatch(entries map[string][]byte) error {

	batch := new(leveldb.Batch)
	for key, value := range entries {
		batch.Put([]byte(key), value)
	}
	return wal.db.Write(batch, nil)
}

// Entries returns an iterator that can be used in range loops to
// iterate over all entries of the WAL.
func (wal *WAL) Entries() iter.Seq2[string, []byte] {
	it := wal.db.NewIterator(nil, nil)
	return wal.getEntries(it)
}

// EntriesWithPrefix returns an iterator that returns entries that match the
// given prefix.
func (wal *WAL) EntriesWithPrefix(prefix string) iter.Seq2[string, []byte] {
	it := wal.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	return wal.getEntries(it)
}

// EntriesBetween returns an iterator that returns entries between the given set of keys.
// It is an open interval - the returned entries include the key 'start' and upto, but excluding
// the key 'end'.
func (wal *WAL) EntriesBetween(start, end string) iter.Seq2[string, []byte] {
	it := wal.db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(end)}, nil)
	return wal.getEntries(it)
}

func (wal *WAL) getEntries(it iterator.Iterator) iter.Seq2[string, []byte] {

	return func(yield func(string, []byte) bool) {
		for it.Next() {
			key := string(it.Key())
			val := bytes.Clone(it.Value())
			if !yield(key, val) {
				break
			}
		}
		it.Release()
		it = nil
	}
}
