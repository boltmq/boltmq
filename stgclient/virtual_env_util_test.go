package stgclient

import (
	"fmt"
	"testing"
)

func TestBuildWithProjectGroup(t *testing.T) {
	fmt.Println(BuildWithProjectGroup("testTopic", "abc"))
	fmt.Println(BuildWithProjectGroup("testTopic", ""))
}

func TestClearProjectGroup(t *testing.T) {
	fmt.Println(ClearProjectGroup("testTopic%PROJECT_abc%", "abc"))
	fmt.Println(ClearProjectGroup("testTopic%PROJECT_abc%", ""))
}
