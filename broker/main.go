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
	"runtime/debug"

	"github.com/boltmq/boltmq/broker/config"
	"github.com/boltmq/boltmq/broker/server"
	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
	"github.com/go-errors/errors"
	daemon "github.com/sevlyar/go-daemon"
)

func main() {
	c := flag.String("c", "", "broker config file, default etc/broker.toml")
	p := flag.String("p", "broker.pid", "pid file, default broker.pid")
	h := flag.Bool("h", false, "help")
	f := flag.Bool("f", false, "run front terminal")
	v := flag.Bool("v", false, "version")

	flag.Parse()
	if *h {
		flag.Usage()
		os.Exit(0)
	}

	if *v {
		fmt.Println("boltmq broker version:", common.Version)
		os.Exit(0)
	}

	if !*f {
		dctx, err := runDaemon(*p)
		if err != nil {
			os.Exit(0)
		}
		defer dctx.Release()
	}

	cfg, err := config.ParseConfig(*c)
	if err != nil {
		fmt.Printf("load config: %s.\n", err)
		os.Exit(0)
	}

	if cfg.Log.CfgFilePath != "" {
		if !*f {
			if err := logger.ConfigAsFile(cfg.Log.CfgFilePath); err != nil {
				fmt.Printf("config %s load failed, %s\n", cfg.Log.CfgFilePath, err)
				os.Exit(0)
			}
		} else {
			if err := logger.ConfigAsBytes([]byte(common.DefaultFrontLogXmlCfg)); err != nil {
				fmt.Printf("front log config load failed, %s\n", err)
				os.Exit(0)
			}
		}
		logger.Infof("config %s load success.", cfg.Log.CfgFilePath)
	}
	debug.SetMaxThreads(100000)

	if cfg.MQHome == "" {
		logger.Info("Please set the BOLTMQ_HOME variable in your environment to match the location of the BlotMQ installation.")
		return
	}
	logger.Infof("Please reset the BOLTMQ_HOME:%s variable in your environment, if it is incorrect.", cfg.MQHome)

	controller, err := server.NewBrokerController(cfg)
	if err != nil {
		fmt.Printf("create broker controller: %s.\n", err)
		logger.Errorf("create broker controller: %s.", err)
		return
	}

	// 注册系统信号量通知。
	system.ExitNotify(func(s os.Signal) {
		controller.Shutdown()
		logger.Info("broker exit, save data...")
		logger.Flush()
		os.Exit(0)
	})

	// 启动BrokerController
	controller.Start()
}

func runDaemon(pidfile string) (*daemon.Context, error) {
	cntxt := &daemon.Context{
		PidFileName: pidfile,
		PidFilePerm: 0644,
		LogFileName: "",
		LogFilePerm: 0640,
		WorkDir:     "./",
		Umask:       027,
		Args:        nil,
	}

	d, err := cntxt.Reborn()
	if err != nil {
		return nil, err
	}
	if d != nil {
		return nil, errors.Errorf("child process not nil.")
	}

	return cntxt, nil
}
