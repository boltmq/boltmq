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
	// example E:/source/src/git.oschina.net/cloudzone/smartgo/stgcommon/namesrv/stgregistry/kvConfig.json
	fmt.Printf("kvConfigPath = %s\n", kvConfigPath)
}
