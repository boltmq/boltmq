package statfs

import (
	"fmt"
	"testing"
)

func TestDiskStatfs(t *testing.T) {
	statfsStatus := DiskStatfs("C:")
	fmt.Printf("%#v\n", statfsStatus)
}
