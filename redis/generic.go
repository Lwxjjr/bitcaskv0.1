package redis

import (
	"errors"
)

func (redis *RedisDataStructure) Del(key []byte) error {
	return redis.db.Delete(key)
}

func (redis *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := redis.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is nil")
	}
	// 第一个字节表示数据类型
	return encValue[0], nil
}
