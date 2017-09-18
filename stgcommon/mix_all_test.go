package stgcommon

import (
	"fmt"
	"testing"
)

func TestString2File(t *testing.T) {
	String2File([]byte("hello word"), "C:/Users/yintongjiang/.smartgo_offsets/a.txt")
}

func TestFormatTimestampMillis(t *testing.T) {
	var stamp int64
	stamp = 1505716870
	value := FormatTimestamp(stamp)
	fmt.Printf("timesmap=%d\t     time.format=%s \n", stamp, value)

	stamp = 1505716870986
	value = FormatTimestamp(stamp)
	fmt.Printf("timesmap=%d\t time.format=%s \n", stamp, value)

}
