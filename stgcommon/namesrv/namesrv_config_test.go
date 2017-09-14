package namesrv

import (
	"fmt"
	"testing"
)

func TestNamesrvConfig(t *testing.T) {
	namesrvConfig := NewNamesrvConfig()

	smartGoHome := namesrvConfig.GetSmartGoHome()
	fmt.Printf("smartGoHome = %s\n", smartGoHome)

	kvConfigPath := namesrvConfig.GetKvConfigPath()
	fmt.Printf("kvConfigPath = %s\n", kvConfigPath)
}
