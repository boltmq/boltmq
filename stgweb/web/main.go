package main

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/cloudcommon-go/web"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/g"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/route"
	"os"
)

const (
	_version = "v1.0.0"
)

func main() {
	v := flag.Bool("v", false, "version")
	help := flag.Bool("h", false, "help")
	flag.Parse()

	if *v {
		fmt.Println(_version)
		os.Exit(0)
	}

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	g.Init()
	web.New(_version).Config(&g.GetConfig().Web).Call(func(ctx *web.Context) {
		ctx.Super().Action = route.Route
	}).End().Run()
}
