package main

import (
	bitcask "bitcask-go"
	"fmt"
	"path/filepath"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = filepath.Join("..", "tmp")
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("key1"))
	if err != nil {
		panic(err)
	}
	_, err = db.Get([]byte("key1"))
	if err != nil {
		panic("key1 should be deleted")
	}
}
