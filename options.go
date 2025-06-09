package bitcask_go

import "os"

type Options struct {
	DirPath      string      // 数据文件存储目录
	DataFileSize int64       // 数据文件大小阈值
	IndexType    IndexerType // 索引类型
	SyncWrites   bool        // 是否每次同步写入数据文件
}

type IndexerType int8

const (
	// B树索引
	BTree IndexerType = iota + 1
	// 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 默认数据文件大小为 256MB
	SyncWrites:   true,              // 默认每次写入都同步到磁盘
	IndexType:    BTree,             // 默认使用 B 树索引
}
