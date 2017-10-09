package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)

func main() {
	stopChannel := make(chan bool, 1) // the 'stopChannel' variable to handle controller.shutdownHook()
	stgbroker.Start(stopChannel)

	for {
		select {
		case <-stopChannel:
			return
		}
	}

}
