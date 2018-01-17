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

const (
	NotReadableBit           = 1      // 禁止读权限
	NotWriteableBit          = 1 << 1 // 禁止写权限
	WriteLogicsQueueErrorBit = 1 << 2 // 逻辑队列是否发生错误
	WriteIndexFileErrorBit   = 1 << 3 // 索引文件是否发生错误
	DiskFullBit              = 1 << 4 // 磁盘空间不足
)

type runningFlags struct {
	flagBits int
}

func (rfs *runningFlags) isReadable() bool {
	if (rfs.flagBits & NotReadableBit) == 0 {
		return true
	}

	return false
}

func (rfs *runningFlags) isWriteable() bool {
	if (rfs.flagBits & (NotWriteableBit | WriteLogicsQueueErrorBit | DiskFullBit | WriteIndexFileErrorBit)) == 0 {
		return true
	}

	return false
}

func (rfs *runningFlags) makeLogicsQueueError() {
	rfs.flagBits |= WriteLogicsQueueErrorBit
}

func (rfs *runningFlags) makeIndexFileError() {
	rfs.flagBits |= WriteIndexFileErrorBit
}

func (rfs *runningFlags) getAndMakeDiskFull() bool {
	result := !((rfs.flagBits & DiskFullBit) == DiskFullBit)
	rfs.flagBits |= DiskFullBit
	return result
}

func (rfs *runningFlags) getAndMakeDiskOK() bool {
	result := !((rfs.flagBits & DiskFullBit) == DiskFullBit)
	rfs.flagBits &= 0xFFFFFFFF ^ DiskFullBit
	return result
}
