package index

import (
	"bytes"

	"bitcask-go/data"

	"github.com/google/btree"
)

// Indexer 抽象索引接口
type Indexer interface {
	// Put 向索引中存储 key 和 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 从索引中获取 key 对应的 数据位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 从索引中删除 key 对应的 数据位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size 返回索引大小
	Size() int

	// Close 关闭索引
	Close() error

	// IndexType 返回索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
	// BPTree B+树索引
	BPtree
)

// NewIndexer 根据索引初始化索引
func NewIndexer(IndexType IndexType, dirPath string, sync bool) Indexer {
	switch IndexType {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return NewART()
	case BPtree:
		return NewBPTree(dirPath, sync)
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

// Iterator 通用索引迭代器
type Iterator interface {
	Rewind()                   // 重置迭代器到起始位置
	Seek(key []byte)           // 定位到指定 key
	Next()                     // 移动到下一个 key
	Valid() bool               // 是否有效，即是否已经遍历完所有 key，用于退出遍历
	Key() []byte               // 获取当前 key
	Value() *data.LogRecordPos // 获取当前 key 对应的值
	Close()                    // 关闭迭代器
}
