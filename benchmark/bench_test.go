package benchmark

import (
	bitcask "bitcask-go"
	"bitcask-go/engine"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

var db *engine.DB

func init() {
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "bitcask-go-benchmark")

	var err error
	db, err = engine.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}
func Benchmark_Put(b *testing.B) {
	// 重置计时器
	b.ResetTimer()
	// 启用内存分配报告
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	// 使用新的随机数生成方式替代 Seed
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(r.Int()))
		if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	// 使用新的随机数生成方式替代 Seed
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(r.Int()))
		assert.Nil(b, err)
	}
}
