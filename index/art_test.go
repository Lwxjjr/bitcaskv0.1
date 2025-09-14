package index

import (
	"testing"

	"bitcask-go/data"
)

func TestART_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("k1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("k2"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("k3"), &data.LogRecordPos{Fid: 1, Offset: 1})
}
