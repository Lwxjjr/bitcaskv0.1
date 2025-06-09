package fio

const DataFilePerm = 0644

// 抽象 IO 管理接口，主要是为了屏蔽上层调用者
// 可以接入不同的 IO 类型，目前支持标准文件 IO
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件
	Write([]byte) (int, error)

	// Sync 同步数据到磁盘
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}

// 初始化 IOManager
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
