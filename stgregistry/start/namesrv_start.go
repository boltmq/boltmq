package main

import (
	"flag"
	"git.oschina.net/cloudzone/smartgo/stgregistry/registry"
	"git.oschina.net/cloudzone/smartgo/stgregistry/start/g"
	"strings"
)

func main() {
	p := flag.Int("p", 0, "registry listen port")
	c := flag.String("c", "", "registry logger config file")
	flag.Parse()

	registryPort := 0
	if *p > 0 {
		registryPort = *p
	}

	configPath := ""
	if strings.TrimSpace(*c) != "" {
		configPath = strings.TrimSpace(*c)
	}
	g.InitLogger(configPath)

	stopChannel := make(chan bool, 1) // the 'stopChannel' variable to handle controller.shutdownHook()
	registry.Startup(stopChannel, registryPort)

	for {
		select {
		case <-stopChannel:
			return
		}
	}
}
