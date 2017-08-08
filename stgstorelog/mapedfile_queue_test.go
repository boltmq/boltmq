// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
package stgstorelog

import (
	"testing"
	"path/filepath"
	"os"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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
