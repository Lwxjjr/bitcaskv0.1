package bitcask_go

import (
	"os"
	"path/filepath"
)

type Options struct {
	DirPath            string      // 数据文件存储目录
	DataFileSize       int64       // 数据文件大小阈值
	IndexType          IndexerType // 索引类型
	SyncWrites         bool        // 是否每次同步写入数据文件
	BytesPerSync       int64       // 累计写入数据量，达到阈值时，同步数据文件
	MMapStartup        bool        // 启动时是否使用 MMap
	DataFileMergeRatio float64     // 数据文件合并的阈值
}

// IteratorOptions 索引迭代器配置
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的 Key，默认为空
	Reverse bool   // 是否倒序
}

type WriteBatchOptions struct {
	MaxBatchSize uint // 一个批次最大写入数据量
	SyncWrites   bool // 是否同步写入数据文件
}

type IndexerType = int8

const (
	// BTree B树索引
	BTree IndexerType = iota + 1
	// ART 自适应基数树索引
	ART
	// BPTree B+树索引, 索引是存储在磁盘上
	BPTree
)

var DefaultOptions = Options{
	DirPath:            filepath.Join(os.TempDir(), "bitcask-go"),
	DataFileSize:       256 * 1024 * 1024, // 默认数据文件大小为 256MB
	SyncWrites:         true,              // 默认每次写入都同步到磁盘
	IndexType:          BTree,             // 默认使用 B 树索引
	BytesPerSync:       0,
	MMapStartup:        true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 1024,
	SyncWrites:   true,
}
