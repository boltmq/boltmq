// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
package fileutil

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

func TestListFilesOrDir(t *testing.T) {
	paths, err := ListFilesOrDir("./../", "ALL")
	if err != nil {
		panic(err.Error())
	}
	for _, val := range paths {
		logger.Info(val)
	}

	println("\n\n\n---------------------------------------")
	paths, err = ListFilesOrDir("./../", "DIR")
	if err != nil {
		panic(err.Error())
	}
	for _, val := range paths {
		logger.Info(val)
	}

	println("\n\n\n---------------------------------------")
	paths, err = ListFilesOrDir("./../", "FILE")
	if err != nil {
		panic(err.Error())
	}
	for _, val := range paths {
		logger.Info(val)
	}
}

func TestEnsureDir(t *testing.T) {
	path := "./tmp/test/001"
	dir := FileDir(path)
	basename := Basename(path)
	println(basename)
	EnsureDir(dir)
}

func TestOffset2FileName(t *testing.T) {
	offset := int64(1024 * 1024 * 1024)
	fileName := Offset2FileName(offset)
	logger.Info(fileName)

	offset = int64(1024 * 1024)
	fileName = Offset2FileName(offset)

	logger.Info(fileName)
}
