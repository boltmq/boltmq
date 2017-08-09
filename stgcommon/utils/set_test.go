package utils

import ("testing"
	set "github.com/deckarep/golang-set"
	"fmt"
)
type ABC struct {
	A string
}
func TestTestTe(t *testing.T) {
	mapA:=make(map[*ABC]bool)
	mapA[&ABC{A:"1"}]=false
	mapA[&ABC{A:"2"}]=false
	mapA[&ABC{A:"3"}]=false
	s:=set.NewSet()
	//s.Add("")
	//s.Add(&ABC{})
	//s.Add(&ABC{})
	fmt.Println(len(s.ToSlice()))
	fmt.Println(len(mapA))
}
