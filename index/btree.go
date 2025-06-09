package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

// BTree 索引，封装google btree
type BTree struct {
	// 写操作不是并发安全的，需要加锁
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 初始化 BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 32 是 btree 的高度，可以根据需要调整
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	return oldItem != nil
}
