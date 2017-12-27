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
package persistent

import (
	"os"
	"path/filepath"
	"strconv"
)

func parentDirectory(dir string) string {
	return filepath.Dir(dir)
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func ensureDir(dirName string) error {
	if len(dirName) == 0 {
		return nil
	}

	exist, err := pathExists(dirName)
	if err != nil {
		return err
	}

	if !exist {
		err := os.MkdirAll(dirName, os.ModePerm)
		if err != nil {
			return err
		}
	}

	return nil
}

// offset2FileName 格式化位20个字符长度的string
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/9
func offset2FileName(offset int64) string {
	fileName := strconv.FormatInt(offset, 10)
	var byteList = make([]byte, 20)
	i := 0
	for i < 20 {
		byteList[i] = 0 + '0'
		i++
	}
	byteList = append(byteList, fileName...)
	index := len(byteList) - 20

	return string(byteList[index : index+20])
}

// listFilesOrDir 返回当前目录下所有的文件或者目录
// Params: path 路径
// Return: listType 从ALL|DIR|FILE选取，分别代表返回所有，目录，文件
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func listFilesOrDir(path string, listType string) ([]string, error) {
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
