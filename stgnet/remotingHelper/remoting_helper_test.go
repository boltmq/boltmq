package remotingHelper

import (
	"testing"
	"strings"
	"fmt"
)

func TestParseChannelRemoteAddr(t *testing.T) {
	remoteAddr := "192.168.2.100:9615/"
	value := remoteAddr
	index := strings.LastIndexAny(remoteAddr, "/")
	if index >= 0 {
		start := 0
		end := index
		value = remoteAddr[start:end]
	}
	fmt.Printf("remoteAddr=%s, index=%d, value=%s", remoteAddr, index, value)
}
