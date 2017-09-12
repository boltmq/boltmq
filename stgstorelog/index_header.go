package stgstorelog

import (
	"bytes"
)

var (
	INDEX_HEADER_SIZE    int32 = 40
	BEGINTIMESTAMP_INDEX int32 = 0
	ENDTIMESTAMP_INDEX   int32 = 8
	BEGINPHYOFFSET_INDEX int32 = 16
	ENDPHYOFFSET_INDEX   int32 = 24
	HASHSLOTCOUNT_INDEX  int32 = 32
	INDEXCOUNT_INDEX     int32 = 36
)

type IndexHeader struct {
	byteBuffer *bytes.Buffer
	// TODO
}

func NewIndexHeader(byteBuffer *bytes.Buffer) *IndexHeader {
	return &IndexHeader{byteBuffer: byteBuffer}
}

func (self *IndexHeader) load() {
	// TODO
}
