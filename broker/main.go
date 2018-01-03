package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"
)

const (
	version = "1.0.0"
)

func main() {
	c := flag.String("c", "configPath", "config broker.toml file")
	h := flag.Bool("h", false, "help")
	v := flag.Bool("v", false, "version")

	flag.Parse()
	if *h {
		flag.Usage()
		os.Exit(0)
	}

	if *v {
		fmt.Println("boltmq version: %s", version)
		os.Exit(0)
	}

	debug.SetMaxThreads(100000)
}
