package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestFileIOManager(t *testing.T) {
	path := filepath.Join("/projectTwo", "a.data")
	fio, err := NewFileIOManager(path)
	defer func() {
		fio.Close()
		destroyFile(path)
	}()

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIOWrite(t *testing.T) {
	path := filepath.Join("/projectTwo", "a.data")
	fio, err := NewFileIOManager(path)
	defer func() {
		fio.Close()
		destroyFile(path)
	}()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	fio.Write([]byte("hello bitcask1\n"))
	t.Log(n, err)

	fio.Write([]byte("hello world1\n"))
	t.Log(n, err)
}

func TestFileIORead(t *testing.T) {
	path := filepath.Join("/projectTwo", "read.data")
	fio, err := NewFileIOManager(path)
	defer func() {
		fio.Close()
		destroyFile(path)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("123"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("1234"))
	assert.Nil(t, err)

	b1 := make([]byte, 3)
	n, err := fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("123"), b1)

	b2 := make([]byte, 4)
	n, err = fio.Read(b2, 3)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("1234"), b2)
}

func TestFileIOSync(t *testing.T) {
	path := filepath.Join("/projectTwo", "read.data")
	fio, err := NewFileIOManager(path)
	defer func() {
		fio.Close()
		destroyFile(path)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIOClose(t *testing.T) {
	path := filepath.Join("/projectTwo", "read.data")
	fio, err := NewFileIOManager(path)
	defer func() {
		fio.Close()
		destroyFile(path)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
