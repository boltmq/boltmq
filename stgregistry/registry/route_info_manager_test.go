package registry

import (
	"fmt"
	"testing"
)

type First struct {
	param map[int]*Second
}

type Second struct {
	value string
}

func NewFirst() *First {
	first := &First{
		param: make(map[int]*Second),
	}
	return first
}

func (self *First) ToString() string {
	if self == nil || self.param == nil || len(self.param) == 0 {
		return "self.param is nil"
	}
	for k, v := range self.param {
		if v == nil {
			return fmt.Sprintf("code=%d, Second is nil", k)
		}
		return fmt.Sprintf("code=%d, Second.value=%s", k, v.value)
	}
	return ""
}

// TestMapInit 测试map获取数据后，value是否存在引用关系  例如 value, ok := map[key]
func TestMapInit(t *testing.T) {
	fmt.Printf("\n")
	first := NewFirst()

	code := 5
	second, ok := first.param[code]
	if !ok || second == nil {
		second = new(Second)
		first.param[code] = second
		fmt.Printf("--> %v %s\n\n", first, first.ToString())
	}

	second.value = "aaaaa"
	fmt.Printf("--> %s\n\n", first.ToString())

	second.value = "bbbbb"
	fmt.Printf("--> %s\n\n", first.ToString())
}
