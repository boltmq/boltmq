package stgcommon

import "testing"

func TestString2File(t *testing.T) {
	String2File([]byte("hello word"), "C:/Users/yintongjiang/.smartgo_offsets/a.txt")
}
