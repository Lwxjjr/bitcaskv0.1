package index

import (
	"bytes"
	"sort"
	"sync"

	"bitcask-go/data"

	goArt "github.com/plar/go-adaptive-radix-tree/v2"
)

// Art 自适应索引树索引
type Art struct {
	tree goArt.Tree
	lock *sync.RWMutex
}

func NewART() *Art {
	return &Art{tree: goArt.New(), lock: new(sync.RWMutex)}
}

func (art *Art) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	val, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if val == nil {
		return nil
	}
	return val.(*data.LogRecordPos)
}

func (art *Art) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	val, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return val.(*data.LogRecordPos)
}

func (art *Art) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	val, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if val == nil {
		return nil, false
	}
	return val.(*data.LogRecordPos), deleted
}

func (art *Art) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

func (art *Art) Close() error {
	return nil
}

func (art *Art) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

// Art 的索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历到的下标位置
	reverse   bool    // 是否反向遍历
	values    []*Item // 存放遍历到的 key+pos 信息
}

func newArtIterator(tree goArt.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node goArt.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (arti *artIterator) Rewind() {
	arti.currIndex = 0
}

func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

func (arti *artIterator) Next() {
	arti.currIndex += 1
}

func (arti *artIterator) Valid() bool {
	return arti.currIndex < len(arti.values)
}

func (arti *artIterator) Key() []byte {
	return arti.values[arti.currIndex].key
}

func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currIndex].pos
}

func (arti *artIterator) Close() {
	arti.values = nil
}
