package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"testing"
)

func TestStartBroker(t *testing.T) {
	stopChan := make(chan bool, 1)
	stgbroker.Start(stopChan)

	for {
		select {
		case <-stopChan:
			return
		}
	}
}
