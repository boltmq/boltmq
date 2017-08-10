// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
package stgstorelog

import (
	"testing"
	"path/filepath"
	"os"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"sort"
)

func TestPathWalk(t *testing.T) {
	filepath.Walk("./", func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		} else if f.IsDir() {
			logger.Info("dir : %v", path)
		} else {
			logger.Info("file : %v", path)
		}
		return nil
	})
}

func TestSort(t *testing.T) {
	var paths []string
	paths = append(paths, "008")
	paths = append(paths, "021")
	paths = append(paths, "001")
	paths = append(paths, "111")
	paths = append(paths, "123")
	for _, val := range paths {
		logger.Info(val)
	}
	sort.Strings(paths)
	logger.Info("-------------------------")
	for _, val := range paths {
		logger.Info(val)
	}
}
