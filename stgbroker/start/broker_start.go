package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)

func main() {
	stgbroker.Start()
	select {}
}
