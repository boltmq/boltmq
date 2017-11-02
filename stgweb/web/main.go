package main

import (
	"flag"
	"fmt"
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

	select {

	}
}
