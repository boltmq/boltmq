package remoting

import (
	"testing"
)

func TestChooseNameseverAddr(t *testing.T) {
	var (
		addrs = []string{"127.0.0.1:10091"}
	)

	client := NewDefalutRemotingClient()
	client.UpdateNameServerAddressList(addrs)

	addr := client.chooseNameseverAddr()

	if addr != addrs[0] {
		t.Errorf("Test failed: choose addr[%s] incorrect, expect[%s]", addr, addrs[0])
	}
}
