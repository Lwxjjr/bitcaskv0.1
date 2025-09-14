package engine

import (
	"bitcask-go"
	"bitcask-go/fio"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bitcask-go/data"
	"bitcask-go/index"
)

const (
	seqNoKey     = "seq.No"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实例
// 涉及读写流程，存放面向用户的操作接口
type DB struct {
	options        bitcask_go.Options // 数据库选项
	mu             *sync.RWMutex
	fileIds        []int                     // 文件 id，只能在加载索引的时候使用
	activeFile     *data.DataFile            // 当前活跃文件，可以用于写入
	olderFiles     map[uint32]*data.DataFile // 旧的数据文件，只能用于读取
	isMerging      bool                      // 是否正在合并
	seqNo          uint64                    // 序列号，全局递增
	seqNoFileExist bool                      // 存储事务 序列号的文件是否存在
	isInitial      bool                      // 是否是第一次初始化此数据目录
	fileLock       *flock.Flock              // 文件锁
	bytesWrite     int64                     // 累计写了多少字节
	reclaimSize    int64                     // 表示有多少数据是无效的
	index          index.Indexer
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum      uint  // key 的数量
	DataFileNum uint  // 数据文件数量
	ReclaimSize int64 // 存储空间回收大小，字节为单位
	DiskSize    int64 // 数据目录所占磁盘空间大小
}

// Open 打开 bitcask 存储引擎实例
func Open(options bitcask_go.Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// 判断数据目录是否存在，不存在的话，则创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, bitcask_go.ErrDatabaseIsUsing
	}

	// 文件存在，但是为空
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化数据库实例
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载对应的数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if options.IndexType != bitcask_go.BPTree {
		// 从 Hint 索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	// 取出当前事务序列号
	if options.IndexType == bitcask_go.BPTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	// 只有启动时才使用 MMap，重置 IO 类型为标准文件 IO
	// 因为这里 MMap 是只读的，todo -> 写
	if db.options.MMapStartup {
		if err := db.resetIOType(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// Close 关闭数据库实例
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			// 在 defer 中 panic 是最后的手段，但通常表明有严重问题
			panic(fmt.Sprintf("failed to unlock the directory: %v", err))
		}
	}()

	// 如果 activeFile 为 nil，则 DB 未进行过写操作，直接返回
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var finalErr error // 用于记录第一个发生的错误

	// 1. 关闭索引
	if err := db.index.Close(); err != nil {
		finalErr = fmt.Errorf("failed to close index: %w", err)
	}

	// 2. 保存当前事务序列号，并确保 seqNoFile 被关闭
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		if finalErr == nil {
			finalErr = fmt.Errorf("failed to open seqNoFile: %w", err)
		}
	} else {
		// 使用 defer 确保 seqNoFile 在任何情况下都能被关闭
		defer func() {
			if err := seqNoFile.Close(); err != nil {
				if finalErr == nil {
					finalErr = fmt.Errorf("failed to close seqNoFile: %w", err)
				}
			}
		}()

		record := &data.LogRecord{
			Key: []byte(seqNoKey),
			Val: []byte(strconv.FormatUint(db.seqNo, 10)),
		}
		encRecord, _ := data.EncodeLogRecord(record)
		if err := seqNoFile.Write(encRecord); err != nil {
			if finalErr == nil {
				finalErr = fmt.Errorf("failed to write to seqNoFile: %w", err)
			}
		}
		if err := seqNoFile.Sync(); err != nil {
			if finalErr == nil {
				finalErr = fmt.Errorf("failed to sync seqNoFile: %w", err)
			}
		}
	}

	// 3. 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		if finalErr == nil {
			finalErr = fmt.Errorf("failed to close active file: %w", err)
		}
	}

	// 4. 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			if finalErr == nil {
				finalErr = fmt.Errorf("failed to close older file %d: %w", file.FileId, err)
			}
		}
	}

	return finalErr
}

//func (db *DB) Close() error {
//	defer func() {
//		if err := db.fileLock.Unlock(); err != nil {
//			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
//		}
//		if err := db.index.Close(); err != nil {
//			panic(fmt.Sprintf("failed to close index"))
//		}
//	}()
//	if db.activeFile == nil {
//		return nil
//	}
//
//	db.mu.Lock()
//	defer db.mu.Unlock()
//
//	// 保存当前事务序列号
//	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
//	if err != nil {
//		return err
//	}
//	record := &data.LogRecord{
//		Key: []byte(seqNoKey),
//		Val: []byte(strconv.FormatUint(db.seqNo, 10)),
//	}
//	encRecord, _ := data.EncodeLogRecord(record)
//	if err := seqNoFile.Write(encRecord); err != nil {
//		return err
//	}
//	if err := seqNoFile.Sync(); err != nil {
//		return err
//	}
//
//	// 关闭当前活跃文件
//	if err := db.activeFile.Close(); err != nil {
//		return err
//	}
//
//	// 关闭旧的数据文件
//	for _, file := range db.olderFiles {
//		if err := file.Close(); err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

// Sync 同步数据到磁盘
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put 写入 Key/Value 数据，Key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否为空
	if len(key) == 0 {
		return bitcask_go.ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(nonTransactionSeqNo, key),
		Val:  value,
		Type: data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// Get 根据 key 获取对应的 value
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, bitcask_go.ErrKeyIsEmpty
	}

	// 从内存数据结构中取得 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果 key 不在内存索引中，则 key 不存在
	if logRecordPos == nil {
		return nil, bitcask_go.ErrKeyNotFound
	}

	// 从数据文件中获取 value
	return db.getValueByPos(logRecordPos)
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 是否为空
	if len(key) == 0 {
		return bitcask_go.ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 logRecord 结构体，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(nonTransactionSeqNo, key),
		Type: data.LogRecordDelete,
	}
	// 追加写入到当前活跃文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	db.reclaimSize += int64(pos.Size)

	// 从内存索引中删除对应的 key
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return bitcask_go.ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	interator := db.index.Iterator(false)
	for interator.Rewind(); interator.Valid(); interator.Next() {
		value, err := db.getValueByPos(interator.Value())
		if err != nil {
			return err
		}
		if !fn(interator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPos 根据索引信息获取 value
func (db *DB) getValueByPos(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, bitcask_go.ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDelete {
		return nil, bitcask_go.ErrKeyNotFound
	}

	return logRecord.Val, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写入活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃文件是否存在，数据库在没有写入的时候是没有文件生成的
	// 如果不存在，则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化文件，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += size
	// 根据配置决定是否需要持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法，必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被损坏
			if err != nil {
				return bitcask_go.ErrDataDirectCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 将文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			// 活跃文件
			db.activeFile = dataFile
		} else {
			// 旧文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDelete {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(oldPos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	currentSeqNo := nonTransactionSeqNo

	// 遍历所有的文件 id，处理文件中的记录
	for i, fid := range db.fileIds {
		fileId := uint32(fid)

		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 不是事务记录
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 SeqNo 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinish {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Pos:    logRecordPos,
						Record: logRecord,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 移动偏移量，下次从这个位置开始
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo

	return nil
}

func checkOptions(Options bitcask_go.Options) error {
	if Options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if Options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if Options.DataFileMergeRatio < 0 || Options.DataFileMergeRatio > 1 {
		return errors.New("database data file merge ratio must be in range [0, 1]")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Val), 10, 64)
	if err != nil {
		return nil
	}
	db.seqNo = seqNo
	db.seqNoFileExist = true
	return os.Remove(fileName)
}

func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size: %v", err))
	}
	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFiles,
		ReclaimSize: db.reclaimSize,
		DiskSize:    dirSize,
	}
}

// Backup 备份数据库
func (db *DB) Backup(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}
