package sync

import (
	concurrent "github.com/fanliao/go-concurrentMap"
)

// Map 线程安全的map
type Map struct {
	// 使用ConcurrentMap，Go1.9版本改用sync/syncmap包实现
	*concurrent.ConcurrentMap
}

// NewMap 返回线程安全map
func NewMap() *Map {
	return &Map{
		ConcurrentMap: concurrent.NewConcurrentMap(),
	}
}

// NewMapFromOtherMap 返回线程安全map
func NewMapFromOtherMap(m map[interface{}]interface{}) *Map {
	return &Map{
		ConcurrentMap: concurrent.NewConcurrentMapFromMap(m),
	}
}
