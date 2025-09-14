package engine

import (
	"bitcask-go"
	"bytes"

	"bitcask-go/index"
)

type Iterator struct {
	indexIter index.Iterator             // 索引迭代器
	options   bitcask_go.IteratorOptions // 迭代器选项
	db        *DB                        // 数据库实例
}

func (db *DB) NewIterator(options bitcask_go.IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   options,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPos(logRecordPos)
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen > 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
