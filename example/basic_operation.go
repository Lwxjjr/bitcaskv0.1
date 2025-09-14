package main

import (
	bitcask "bitcask-go"
	"bitcask-go/engine"
	"path/filepath"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = filepath.Join(".", "tmp")
	db, err := engine.Open(opts)
	if err != nil {
		panic(err)
	}

	// 插入1000条数据
	//for i := 1000; i <= 2000; i++ {
	//	key := []byte(fmt.Sprintf("key%d", i))
	//	value := []byte(fmt.Sprintf("value%d", i))
	//	err = db.Put(key, value)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	err = db.Close()
	if err != nil {
		panic(err)
	}
}
