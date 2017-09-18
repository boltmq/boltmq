package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"testing"
)

func TestNamesrvConfig(t *testing.T) {
	namesrvConfig := NewNamesrvConfig()

	smartGoHome := namesrvConfig.GetSmartGoHome()
	fmt.Printf("smartGoHome = %s\n", smartGoHome)

	kvConfigPath := namesrvConfig.GetKvConfigPath()
	fmt.Printf("kvConfigPath = %s\n", kvConfigPath)

	kvConfigDir := namesrvConfig.GetKvConfigDir()
	fmt.Printf("kvConfigDir = %s\n", kvConfigDir)

	kvConfigName := namesrvConfig.GetKvConfigName()
	fmt.Printf("kvConfigName = %s\n", kvConfigName)

	ok, err := stgcommon.ExistsFile(kvConfigPath)
	if err == nil && ok {
		fmt.Printf("exists file [%s]\n", kvConfigPath)
	} else {
		fmt.Printf("exists not file[%s]\n", kvConfigPath)
	}
}
