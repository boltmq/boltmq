package main

import (
	"git.oschina.net/cloudzone/smartgo/stgremoting/netm"
)

func main() {
	b := netm.NewBootstrap()
	b.Bind("0.0.0.0", 8000).Sync()
}
