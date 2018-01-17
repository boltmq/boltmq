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
	"fmt"
	"os"

	"github.com/boltmq/boltmq/common/statfs"
)

// 获取磁盘分区空间使用率
func GetDiskPartitionSpaceUsedPercent(path string) (percent float64) {
	if path == "" {
		return -1
	}

	isExits, err := PathExists(path)
	if err != nil {
		return -1
	}

	if !isExits {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return -1
		}
	}

	diskStatus, err := statfs.DiskUsage(path)
	if err != nil {
		return -1
	}

	percent = float64(diskStatus.Used) / float64(diskStatus.All)
	return
}

func GetStorePathCommitLog(rootDir string) string {
	return fmt.Sprintf("%s%ccommitlog", rootDir, os.PathSeparator)
}

func GetStorePathConsumeQueue(rootDir string) string {
	return fmt.Sprintf("%s%cconsumequeue", rootDir, os.PathSeparator)
}

func GetStorePathIndex(rootDir string) string {
	return fmt.Sprintf("%s%cindex", rootDir, os.PathSeparator)
}

func GetStorePathCheckpoint(rootDir string) string {
	return fmt.Sprintf("%s%ccheckpoint", rootDir, os.PathSeparator)
}

func GetStorePathAbortFile(rootDir string) string {
	return fmt.Sprintf("%s%cabort", rootDir, os.PathSeparator)
}

func GetDelayOffsetStorePath(rootDir string) string {
	return fmt.Sprintf("%s%cconfig%cdelayOffset.json", rootDir, os.PathSeparator, os.PathSeparator)
}

func GetTranStateTableStorePath(rootDir string) string {
	return fmt.Sprintf("%s%ctransaction%cstatetable", rootDir, os.PathSeparator, os.PathSeparator)
}

func GetTranRedoLogStorePath(rootDir string) string {
	return fmt.Sprintf("%s%ctransaction%credolog", rootDir, os.PathSeparator, os.PathSeparator)
}
