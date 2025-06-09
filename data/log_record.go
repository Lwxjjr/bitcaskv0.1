package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota // 普通数据
	LogRecordDelete                      // 删除数据
)

// 头部字节
// crc、type、keySize、valSize
//
//	4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = crc32.Size + 1 + binary.MaxVarintLen32*2

// 写入到数据文件的记录
// 追加写，类似于日志，所以叫 LogRecord
type LogRecord struct {
	Key  []byte
	Val  []byte
	Type LogRecordType
}

// LogRecord 的头部信息
type LogRecordHeader struct {
	crc        uint32        // crc 校验和
	recordType LogRecordType // 记录类型
	keySize    uint32        // key 大小
	valSize    uint32        // value 大小
}

// LogRecordPos 数据内存索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID，表示将数据存储在哪个文件中
	Offset int64  // 偏移，表示将数据存储在文件中的偏移位置
}

// EncodeLogRecord 对 logRecord 编码，返回字节数组及其长度
// 转化为数据文件中的一条日志记录
// +--------+-----------+---------+---------+-----+-----+
// | crc 	| 	type 	| keySize | valSize | key | val |
// +--------+-----------+---------+---------+-----+-----+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化头部信息
	header := make([]byte, maxLogRecordHeaderSize)

	// 从第五个字节开始写
	header[4] = logRecord.Type
	var index = 5
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Key)))
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Val)))

	var size = index + len(logRecord.Key) + len(logRecord.Val)
	encBytes := make([]byte, size)

	// 将header部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将key和val拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Val)

	// 计算 logRecord 的CRC校验和
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	keySize, n := binary.Uvarint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valSize, n := binary.Uvarint(buf[index:])
	header.valSize = uint32(valSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Val)

	return crc
}
