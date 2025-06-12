package utils

import "testing"

func TestGetTestKV(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Log(string(GetTestKey(i)))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Log(string(RandomValue(10)))
	}
}
