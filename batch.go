package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn_fin")

// WriteBatch 批量保证原子写数据
type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	options       WriteBatchOptions
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		mu:            new(sync.Mutex),
		db:            db,
		options:       options,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 暂存 LogRecord
	logRecord := &data.LogRecord{
		Key: key,
		Val: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] == nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 将暂存的数据写入到磁盘中，并更新索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > int(wb.options.MaxBatchSize) {
		return ErrExceedMaxBatchSize
	}

	// 加锁保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 写入数据
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:  logRecordKeyWithSeq(record.Key, seqNo),
			Val:  record.Val,
			Type: record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinish,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置是否同步写入磁盘
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDelete {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// key + SeqNo 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	encKey := make([]byte, len(key)+n)
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 中的key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
