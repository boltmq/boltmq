package stgstorelog

import "sync"

type AllocateRequest struct {
	filePath  string
	fileSize  int64
	waitGroup sync.WaitGroup
	mapedFile *MapedFile
}

func NewAllocateRequest(filePath string, fileSize int64) *AllocateRequest {
	request := new(AllocateRequest)
	request.filePath = filePath
	request.fileSize = fileSize
	request.waitGroup = sync.WaitGroup{}
	request.waitGroup.Add(1)

	return request
}

func (self *AllocateRequest) compareTo(request *AllocateRequest) int {
	if self.fileSize < request.fileSize {
		return 1
	}

	if self.fileSize > request.fileSize {
		return -1
	}

	return 0
}
