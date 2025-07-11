package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	t.Log(os.TempDir())

	dataFile2, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("abc"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("111"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 3)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("hello world"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 222)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 单条LogRecord
	rec1 := &LogRecord{
		Key: []byte("name"),
		Val: []byte("bitcask"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	dataFile.Write(res1)
	assert.Nil(t, err)
	t.Log(size1)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.NotNil(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	// 多条LogRecord
	rec2 := &LogRecord{
		Key: []byte("name"),
		Val: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:  []byte("1"),
		Val:  []byte(""),
		Type: LogRecordDelete,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	t.Log(size3)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)

}
