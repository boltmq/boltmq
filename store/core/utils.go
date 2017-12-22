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
package core

import (
	"os"
	"path/filepath"
	"strings"
)

func parentDirectory(dir string) string {
	return substr(dir, 0, strings.LastIndex(dir, pathSeparator()))
}

func pathSeparator() string {
	return filepath.FromSlash(string(os.PathSeparator))
}

func substr(s string, pos, length int) string {
	runes := []rune(s)
	size := pos + length
	if size > len(runes) {
		size = len(runes)
	}
	return string(runes[pos:size])
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
