package namesrv

import (
	"fmt"
	"testing"
)

func TestNamesrvConfig(t *testing.T) {
	smartGoHome := GetSmartGoHome()
	fmt.Printf("smartGoHome = %s\n", smartGoHome)

	kvConfigPath := GetKvConfigPath()
	fmt.Printf("kvConfigPath = %s\n", kvConfigPath)

}