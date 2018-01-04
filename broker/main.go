// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/boltmq/boltmq/broker/config"
	"github.com/boltmq/boltmq/broker/server"
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

	controller, err := server.NewBrokerController(cfg)
	if err != nil {
		fmt.Printf("create broker controller: %s.\n", err)
		logger.Errorf("create broker controller: %s.", err)
		return
	}
	fmt.Println("->", cfg, controller)
	//debug.SetMaxThreads(100000)
}
