package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("bitcask"),
		Type: LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// val 为空
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
}

func TestDecodeLogRecord(t *testing.T) {
	headerBuf1 := []byte{204, 206, 78, 237, 0, 4, 7}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(3981364940), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(7), h1.valSize)

	headerBuf2 := []byte{114, 60, 154, 121, 0, 4, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(2040151154), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valSize)

}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("bitcask"),
		Type: LogRecordNormal,
	}
	headerBuf1 := []byte{204, 206, 78, 237, 0, 4, 7}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(3981364940), crc1)
}
