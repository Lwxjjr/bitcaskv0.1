package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// 抽象索引接口
type Indexer interface {
	// Put 向索引中存储 key 和 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 从索引中获取 key 对应的 数据位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 从索引中删除 key 对应的 数据位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewBTreeIndexer 根据索引初始化索引
func NewIndexer(IndexType IndexType) Indexer {
	switch IndexType {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
