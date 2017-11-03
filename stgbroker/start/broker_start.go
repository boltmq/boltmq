package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"

	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	"github.com/toolkits/file"
)

func main() {
	debug.SetMaxThreads(100000)

	c := flag.String("c", "configPath", "Broker config *.toml file")
	h := flag.Bool("h", false, "help")
	v := flag.Bool("v", false, "version")

	flag.Parse()

	if *h {
		flag.Usage()
		os.Exit(0)
	}

	if *v {
		fmt.Println(mqversion.GetCurrentDesc())
		os.Exit(0)
	}

	// 使用-c指令,但是没有给默认值
	if *c == "" {
		fmt.Println("use -c to specify broker config file. eg: -c /home/smartgo-bin/conf/smartgoBroker.toml")
		os.Exit(0)
	}

	// 没有使用-c指令，那么*c="configPath"表示默认值
	cfgPath := ""
	if *c != "configPath" {
		if !file.IsExist(*c) {
			fmt.Println("use -c to valid broker config path. eg: -c /home/smartgo-bin/conf/broker-a.toml")
			os.Exit(0)
		} else {
			cfgPath = *c
		}
	}

	stopChannel := make(chan bool, 1) // the 'stopChannel' variable to handle controller.shutdownHook()
	stgbroker.Start(stopChannel, cfgPath)

	for {
		select {
		case <-stopChannel:
			return
		}
	}

}
