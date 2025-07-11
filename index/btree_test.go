package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBtreePut(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)
}

func TestBtreeGet(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)

	pos2 := bt.Get(nil)
	t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(100), pos2.Offset)
}

func TestBtreeDelete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// 1. BTree 为空
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2， BTree 有数据的情况
	bt1.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	iter2 := bt1.Iterator(false)
	// t.Log(iter2.Valid())
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3. BTree 存在多条数据
	bt1.Put([]byte("111"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("333"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("222"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter4 := bt1.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4. 测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("2")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5. 反向测试 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("2")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}
}
