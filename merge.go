package bitcask_go

import (
	"os"
	"path"
	"path/filepath"
	"sort"

	"bitcask-go/data"
)

const mergeDirName = "-merge"

// Merge 清理无效数据，生成Hint文件
func (db *DB) Merge() error {
	// 如果数据库为空，则无需合并
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	if db.isMerging {
		defer db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 将当前活跃文件转换为旧数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// 打开一个新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 从小到到大进行 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过merge，删除
	if _, err := os.Stat(mergePath); err != nil {
		return err
	}

	// 新建一个 merge path 目录
	os.Mkdir()
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}
