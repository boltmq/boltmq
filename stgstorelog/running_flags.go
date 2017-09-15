package stgstorelog

const (
	NotReadableBit           = 1      // 禁止读权限
	NotWriteableBit          = 1 << 1 // 禁止写权限
	WriteLogicsQueueErrorBit = 1 << 2 // 逻辑队列是否发生错误
	WriteIndexFileErrorBit   = 1 << 3 // 索引文件是否发生错误
	DiskFullBit              = 1 << 4 // 磁盘空间不足
)

type RunningFlags struct {
	flagBits int
}

func (self *RunningFlags) isReadable() bool {
	if (self.flagBits & NotReadableBit) == 0 {
		return true
	}

	return false
}

func (self *RunningFlags) isWriteable() bool {
	if (self.flagBits & (NotWriteableBit | WriteLogicsQueueErrorBit | DiskFullBit | WriteIndexFileErrorBit)) == 0 {
		return true
	}

	return false
}

func (self *RunningFlags) makeLogicsQueueError() {
	self.flagBits |= WriteLogicsQueueErrorBit
}

func (self *RunningFlags) makeIndexFileError() {
	self.flagBits |= WriteIndexFileErrorBit
}

func (self *RunningFlags) getAndMakeDiskFull() bool {
	result := !((self.flagBits & DiskFullBit) == DiskFullBit)
	self.flagBits |= DiskFullBit
	return result
}

func (self *RunningFlags) getAndMakeDiskOK() bool {
	result := !((self.flagBits & DiskFullBit) == DiskFullBit)
	self.flagBits &= 0xFFFFFFFF ^ DiskFullBit
	return result
}
