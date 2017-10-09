package main

import (
	"git.oschina.net/cloudzone/smartgo/stgregistry/registry"
	"git.oschina.net/cloudzone/smartgo/stgregistry/start/g"
)

func main() {
	g.InitLogger()

	stopChannel := make(chan bool, 1) // the 'stopChannel' variable to handle controller.shutdownHook()
	registry.Startup(stopChannel)

	for {
		select {
		case <-stopChannel:
			return
		}
	}
}
