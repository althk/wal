package wal

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDBPath = "testdb"

func cleanup() {
	_ = os.RemoveAll(testDBPath)
}

func closeWAL(wal *WAL) {
	_ = wal.Close()
}

func TestWal_New_Close(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	require.NoError(t, err)
	require.NotNil(t, w)

	err = w.Close()
	assert.NoError(t, err)
}

func TestWal_Put_Get(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	require.NoError(t, err)
	defer closeWAL(w)

	err = w.Put("key1", []byte("value1"))
	assert.NoError(t, err)

	value, err := w.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	value, err = w.Get("nonexistent")
	require.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, value)
}

func TestWal_Delete(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	require.NoError(t, err)
	defer closeWAL(w)

	err = w.Put("key1", []byte("value1"))
	assert.NoError(t, err)

	err = w.Delete("key1")
	assert.NoError(t, err)

	value, err := w.Get("key1")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, value)
}

func TestWal_PutBatch(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	assert.NoError(t, err)
	defer closeWAL(w)

	batch := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}
	err = w.PutBatch(batch)
	assert.NoError(t, err)

	for key, expectedValue := range batch {
		value, err := w.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

func TestWal_Entries(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	assert.NoError(t, err)
	defer closeWAL(w)

	data := map[string][]byte{
		"a": []byte("value1"),
		"b": []byte("value2"),
		"c": []byte("value3"),
	}
	err = w.PutBatch(data)
	assert.NoError(t, err)

	got := map[string][]byte{}
	for k, v := range w.Entries() {
		got[k] = v
	}

	assert.Equal(t, data, got)
}

func TestWal_EntriesWithPrefix(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	assert.NoError(t, err)
	defer closeWAL(w)

	data := map[string][]byte{
		"prefix1_a": []byte("value1"),
		"prefix1_b": []byte("value2"),
		"prefix2_c": []byte("value3"),
	}
	err = w.PutBatch(data)
	assert.NoError(t, err)

	got := map[string][]byte{}
	for k, v := range w.EntriesWithPrefix("prefix1") {
		got[k] = v
	}

	want := map[string][]byte{
		"prefix1_a": []byte("value1"),
		"prefix1_b": []byte("value2"),
	}
	assert.Equal(t, want, got)
}

func TestWal_EntriesBetween(t *testing.T) {
	defer cleanup()

	w, err := New(testDBPath)
	require.NoError(t, err)
	defer closeWAL(w)

	data := map[string][]byte{
		"a": []byte("value1"),
		"b": []byte("value2"),
		"c": []byte("value3"),
		"d": []byte("value4"),
	}
	err = w.PutBatch(data)
	require.NoError(t, err)

	got := map[string][]byte{}
	for k, v := range w.EntriesBetween("a", "d") {
		got[k] = v
	}

	want := map[string][]byte{
		"a": []byte("value1"),
		"b": []byte("value2"),
		"c": []byte("value3"),
	}
	assert.Equal(t, want, got)
}
