package main

import (
	"git.oschina.net/cloudzone/smartgo/stgregistry/registry"
)

func main() {

	registry.Startup()
	select {}
}
