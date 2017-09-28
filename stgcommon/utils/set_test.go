package utils

import (
	"fmt"
	set "github.com/deckarep/golang-set"
	"testing"
)

type ABC struct {
	A string
}

func TestTestTe(t *testing.T) {

	mapA := make(map[*ABC]bool)
	mapA[&ABC{A: "1"}] = false
	mapA[&ABC{A: "2"}] = false
	mapA[&ABC{A: "3"}] = false
	b := set.NewSet()
	b.Add("a")
	b.Add("b")
	s := set.NewSet()
	s.Add("a")
	s.Add("b")
	s.Add(b)
	for val := range s.Iterator().C {
		fmt.Println(val)
	}
	//s.Add("")
	//s.Add(&ABC{})
	//s.Add(&ABC{})
	fmt.Println(len(s.ToSlice()))
	fmt.Println(len(mapA))
}
