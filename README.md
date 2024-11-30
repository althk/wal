# WAL

A simple Write-Ahead-Log built on LevelDB.

Using LevelDB gives several benefits without writing any additional code, like compression and compaction.
See [LevelDB](https://github.com/google/leveldb/tree/main?tab=readme-ov-file#features) for more details.

## Usage

```go

package main

import (
	"github.com/althk/wal"
)

func main() {
	wal, err := wal.New("/path/to/db")
	if err != nil {
		// handle error
    }
	defer wal.Close()
	
	err = wal.Put("key1", []byte("val1"))
	err = wal.PutBatch(
		map[string][]byte{
			"key2", []byte(100),
			"key3", []byte(false),
			"prefix_a", []byte("a"),
			"prefix_b", []byte("b"),
		}
	)
	
	for k, v := wal.Entries() {
	    fmt.Printf("k=%v, v=%v\n", k, v)	
    }
}
```