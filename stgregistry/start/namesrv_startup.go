package main

import "git.oschina.net/cloudzone/smartgo/stgregistry"

func main() {

	stgregistry.Start()
	select {}
}
