package stgcommon

import (
	"testing"
	"fmt"
)

func TestCompress(t *testing.T) {
	src:=[]byte("say hello!")
	fmt.Println(len(src))
	cSrc:=Compress(src)
	fmt.Printf(len(cSrc))
}
