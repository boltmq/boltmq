// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
package fileutil

import (
	"path/filepath"
	"os"
	"path"
	"strconv"
)

// ListFilesOrDir 返回当前目录下所有的文件或者目录
// Params: path 路径
// Return: listType 从ALL|DIR|FILE选取，分别代表返回所有，目录，文件
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func ListFilesOrDir(path string, listType string) ([]string, error) {
	var pathSlice []string
	err := filepath.Walk(path, func(path2 string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			if listType == "DIR" || listType == "ALL" {
				pathSlice = append(pathSlice, path2)
			}
		} else if listType == "FILE" || listType == "ALL" {
			pathSlice = append(pathSlice, path2)
		}
		return nil
	})
	return pathSlice, err
}

func FileSize(file *os.File) (int64, error) {
	f, error := file.Stat()
	if error != nil {
		return -1, error
	}
	return f.Size(), nil

}

// get filepath base name
func Basename(fp string) string {
	return path.Base(fp)
}

// get filepath dir name
func FileDir(fp string) string {
	return path.Dir(fp)
}

func InsureDir(fp string) error {
	if IsExist(fp) {
		return nil
	}
	return os.MkdirAll(fp, os.ModePerm)
}

// EnsureDir dir if not exist
// 如果path为"./tmp/test/test.txt"则只会创建./tmp/test/目录
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func EnsureDir(fp string) error {
	dir := FileDir(fp)
	return os.MkdirAll(dir, os.ModePerm)
}

// IsExist checks whether a file or directory exists.
// It returns false when the file or directory does not exist.
func IsExist(fp string) bool {
	_, err := os.Stat(fp)
	return err == nil || os.IsExist(err)
}

// Offset2FileName 格式化位20个字符长度的string
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/9
func Offset2FileName(offset int64) string {
	fileName := strconv.FormatInt(offset, 10)
	var byteList = make([]byte, 20)
	i := 0
	for i < 20 {
		byteList[i] = 0 + '0'
		i ++
	}
	byteList = append(byteList, fileName...)
	index := len(byteList) - 20

	return string(byteList[index:index+20])
}
