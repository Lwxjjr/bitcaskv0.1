package bitcask_go

import (
	"errors"
)

var (
	ErrKeyIsEmpty = errors.New("key is empty")
	// 索引更新失败
	ErrIndexUpdateFailed = errors.New("index update failed")
	// key 不存在
	ErrKeyNotFound = errors.New("key not found")
	// 索引文件不存在
	ErrDataFileNotFound = errors.New("data file not found")
	// 文件损坏
	ErrDataDirectCorrupted = errors.New("the database directory maybe corrupted")
	// 批量写入超出限制
	ErrExceedMaxBatchSize    = errors.New("exceed max engine size")
	ErrMergeIsProgress       = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing       = errors.New("the database directory is used by another process")
	ErrMergeRatioUnreached   = errors.New("the merge ratio is not reached")
	ErrNoEnoughSpaceForMerge = errors.New("no enough disk space for merge")
)
