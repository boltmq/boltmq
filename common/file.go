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
package common

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

func ParentDirectory(dir string) string {
	return filepath.Dir(dir)
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func EnsureDir(dirName string) error {
	if len(dirName) == 0 {
		return nil
	}

	exist, err := PathExists(dirName)
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

func String2File(data []byte, flPath string) error {
	filePath := filepath.FromSlash(flPath)
	tmpFilePath := filePath + ".tmp"
	bakFilePath := filePath + ".bak"

	err := create2WriteFile(data, tmpFilePath)
	if err != nil {
		return err
	}

	oldData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = create2WriteFile(oldData, bakFilePath)
	if err != nil {
		return err
	}

	// 删除原文件
	err = os.Remove(filePath)
	if err != nil {
		return err
	}

	// 重命临时文件
	return os.Rename(tmpFilePath, filePath)
}

func create2WriteFile(data []byte, filePath string) error {
	err := EnsureDir(filepath.Dir(filePath))
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

// CreateNullFile 创建文件
func CreateNullFile(filePath string) (bool, error) {
	err := EnsureDir(filepath.Dir(filePath))
	if err != nil {
		return false, err
	}

	_, err = os.Create(filePath)
	if err != nil {
		return false, err
	}

	return true, nil
}

// File2String 读取文件内容
func File2String(filePath string) (data string, err error) {
	bf, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	return string(bf), nil
}
