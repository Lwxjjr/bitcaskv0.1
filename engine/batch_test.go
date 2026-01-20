package engine

import (
	bitcask_go "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_WriteBatch1(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-engine-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后并不提交
	wb := db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask_go.ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	val2, err := db.Get(utils.GetTestKey(1))
	t.Log(val2)
	t.Log(err)
}

func Test_WriteBatch2(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-engine-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(3), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask_go.ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), db2.seqNo)
}

func Test_WriteBatch3(t *testing.T) {
	opts := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-engine-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	for i := 0; i < 10000; i++ {
		err = wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

}
