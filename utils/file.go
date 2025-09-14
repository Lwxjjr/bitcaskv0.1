package utils

import (
	"golang.org/x/sys/windows"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// DirSize 获取目录大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取磁盘剩余可用空间大小
// 1. Unix
//func AvailableDiskSize() (uint64, error) {
//	wd, err := syscall.Getwd()
//	if err != nil {
//		return 0, err
//	}
//	var stat syscall.Statfs_t
//	if err = syscall.Statfs(wd, &stat); err != nil {
//		return 0, err
//	}
//	return stat.Bavail * uint64(stat.Bsize), nil
//}

// 2. Windows
func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	var freeBytesAvailableToCaller uint64
	// GetDiskFreeSpaceExW 是 Windows API，用于获取磁盘空间信息
	// 它需要一个路径来确定是哪个驱动器
	err = windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(wd), // 路径
		&freeBytesAvailableToCaller,  // 对调用者可用的字节数
		nil,                          // 磁盘总字节数 (我们不需要，所以传 nil)
		nil,                          // 磁盘总可用字节数 (我们不需要，所以传 nil)
	)
	if err != nil {
		return 0, err
	}

	return freeBytesAvailableToCaller, nil
}

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return nil
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
