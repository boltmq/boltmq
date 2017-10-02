package main

import (
	"git.oschina.net/cloudzone/smartgo/stgregistry/registry"
)

func main() {
	stopChannel := make(chan bool, 1) // the 'stopChannel' variable to handle controller.shutdownHook()
	registry.Startup(stopChannel)
	for {
		select {
		case <-stopChannel:
			return
		}
	}
}
