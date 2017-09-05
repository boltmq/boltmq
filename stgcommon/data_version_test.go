package stgcommon

import (
	"fmt"
	"testing"
)

func TestNewClientConfig(t *testing.T) {
	dataVersion := NewDataVersion()
	for i := 0; i < 10000; i++ {
		dataVersion.NextVersion()
		fmt.Println(dataVersion.ToString())
	}
}
