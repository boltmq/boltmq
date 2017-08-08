# 线程安全map

### 说明

* 使用ConcurrentMap实现，Go1.9版本改用sync/syncmap包实现
* map中key与value使用指针，请实现HashBytes与Equals接口

### 代码示例
```go
package main

import (
	"fmt"

	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
)

func main() {
	demoInt()
	demoStruct()
}

func demoInt() {
	m := sync.NewMap()

	previou, err := m.Put(1, 10) //return nil, nil
	fmt.Println("put val:", previou, err)
	previou, err = m.PutIfAbsent(1, 20) //return 10, nil
	fmt.Println("put if absent val:", previou, err)

	val, err := m.Get(1) //return 10, nil
	fmt.Println("get val:", val, err)
	s := m.Size() //return 1
	fmt.Println("map size:", s)

	err = m.PutAll(map[interface{}]interface{}{
		1: 100,
		2: 200,
	})
	fmt.Println("putalll:", err)

	ok, err := m.RemoveEntry(1, 100) //return true, nil
	fmt.Println("removeEntry:", ok, err)

	previou, err = m.Replace(2, 20) //return 200, nil
	fmt.Println("replace:", previou, err)
	ok, err = m.CompareAndReplace(2, 200, 20) //return false, nil
	fmt.Println("CompareAndReplace:", previou, err)

	previou, err = m.Remove(2) //return 20, nil
	fmt.Println("remove:", previou, err)

	m.Clear()
	s = m.Size() //return 0
	fmt.Println("clear map size:", s)
}

type user struct {
	id   string
	Name string
}

func (u *user) HashBytes() []byte {
	return []byte(u.id)
}

func (u *user) Equals(v2 interface{}) (equal bool) {
	u2, ok := v2.(*user)
	return ok && u.id == u2.id
}

type userInfo struct {
	id     string
	Name   string
	Street string
	Age    int
}

func (u *userInfo) HashBytes() []byte {
	return []byte(u.id)
}

func (u *userInfo) Equals(v2 interface{}) (equal bool) {
	u2, ok := v2.(*userInfo)
	return ok && u.id == u2.id
}

func demoStruct() {
	m := sync.NewMap()

	k := &user{id: "0001", Name: "jerrylou"}
	v := &userInfo{id: "0001", Name: "jerrylou", Street: "tianfusanjie", Age: 16}
	previou, err := m.Put(k, v)
	fmt.Println("put val:", previou, err)

	previou, err = m.PutIfAbsent(k, v)
	fmt.Println("put if absent val:", previou, err)

	val, err := m.Get(k)
	fmt.Println("get val:", val, err)
	s := m.Size()
	fmt.Println("map size:", s)

	err = m.PutAll(map[interface{}]interface{}{
		k: v,
		&user{id: "0002", Name: "jack"}: &userInfo{id: "0002", Name: "jack", Street: "tianfusanjie", Age: 26},
	})
	fmt.Println("putalll:", err)

	ok, err := m.RemoveEntry(k, v)
	fmt.Println("removeEntry:", ok, err)

	previou, err = m.Replace(&user{id: "0002", Name: "jack"}, &userInfo{id: "0002", Name: "jack wang", Street: "tianfusanjie", Age: 28})
	fmt.Println("replace:", previou, err)

	previou, err = m.Remove(&user{id: "0002", Name: "jack"})
	fmt.Println("remove:", previou, err)

	m.Clear()
	s = m.Size()
	fmt.Println("clear map size:", s)
}
```
