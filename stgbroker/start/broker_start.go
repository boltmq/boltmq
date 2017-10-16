package main

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"os"
)

func main() {

	c := flag.String("c", "configFile", "Broker config properties file")
	flag.Parse()

	// 使用-c指令,但是没有给默认值
	if *c == "" {
		fmt.Println("use -c to specify broker config file. eg: -c /home/smartgo-bin/conf/smartgoBroker.toml")
		os.Exit(0)
	}

	// 没有使用-c指令，那么c="configFile"表示默认值
	cfgName := ""
	if *c != "configFile" {
		cfgName = *c
	}

	stopChannel := make(chan bool, 1) // the 'stopChannel' variable to handle controller.shutdownHook()
	stgbroker.Start(stopChannel, cfgName)

	for {
		select {
		case <-stopChannel:
			return
		}
	}

}
