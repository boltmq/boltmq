package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/boltmq/boltmq/broker/config"
	"github.com/boltmq/common/logger"
)

const (
	version = "1.0.0"
)

func main() {
	c := flag.String("c", "", "broker config file, default etc/broker.toml")
	h := flag.Bool("h", false, "help")
	v := flag.Bool("v", false, "version")

	flag.Parse()
	if *h {
		flag.Usage()
		os.Exit(0)
	}

	if *v {
		fmt.Println("boltmq broker version:", version)
		os.Exit(0)
	}

	cfg, err := config.ParseConfig(*c)
	if err != nil {
		fmt.Printf("load config: %s.\n", err)
		logger.Errorf("load config: %s.", err)
		return
	}
	logger.Info("load config success.")

	fmt.Println("->", cfg)
	//debug.SetMaxThreads(100000)
}
