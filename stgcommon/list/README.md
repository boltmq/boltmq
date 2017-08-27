#Buffer Linked List

```go
package main

import (
	"bytes"
	"fmt"

	"git.oschina.net/cloudzone/smartgo/stgcommon/list"
)

func main() {
	bufferList := list.NewBufferLinkedList()
	bufferList.Add(bytes.NewBufferString("a"))                             // ["a"]
	bufferList.Add(bytes.NewBufferString("c"), bytes.NewBufferString("b")) // ["a","c","b"]
	buffers := bufferList.Values()
	for _, buf := range buffers {
		fmt.Println("->", string(buf.Bytes()))
	}

	buf, ok := bufferList.Get(100) // nil,false
	if !ok {
		fmt.Println("not found", buf, ok)
	}

	ok = bufferList.Contains(bytes.NewBufferString("a"))
	if !ok {
		fmt.Println("not contains", ok)
	}

	bufferList.Swap(0, 1)
	buffers = bufferList.Values()
	for _, buf := range buffers {
		fmt.Println("->", string(buf.Bytes()))
	}

	bufferList.Remove(2)
	bufferList.Remove(1)
	bufferList.Remove(0)
	buffers = bufferList.Values()
	for _, buf := range buffers {
		fmt.Println("->", string(buf.Bytes()))
	}

	size := bufferList.Size() // 0
	fmt.Println("size->", size)

	isEmpty := bufferList.Empty()
	fmt.Println("isEmpty->", isEmpty)

	bufferList.Add(bytes.NewBufferString("a"))       // ["a"]
	bufferList.Clear()                               // []
	bufferList.Insert(0, bytes.NewBufferString("b")) // ["b"]
	bufferList.Insert(0, bytes.NewBufferString("a")) // ["a","b"]
	buffers = bufferList.Values()
	for _, buf := range buffers {
		fmt.Println("->", string(buf.Bytes()))
	}
}
```
