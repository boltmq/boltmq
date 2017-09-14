package mqversion

import (
	"fmt"
	"testing"
)

func TestGetVersionDesc(t *testing.T) {
	versionCode := 79
	versionDesc := GetVersionDesc(versionCode)
	fmt.Printf("version [code=%d, desc=\"%s\"]", versionCode, versionDesc)
}
