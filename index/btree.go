package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree 的索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历到的下标位置
	reverse   bool    // 是否反向遍历
	values    []*Item // 存放遍历到的 key+pos 信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	}
	tree.Ascend(saveValues)
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
