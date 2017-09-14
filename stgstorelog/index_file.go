package stgstorelog

var (
	HASH_SLOT_SIZE int32 = 4
	INDEX_SIZE     int32 = 20
	INVALID_INDEX  int32 = 0
)

type IndexFile struct {
	hashSlotNum      int32
	indexNum         int32
	mapedFile        *MapedFile
	mappedByteBuffer *MappedByteBuffer
	// TODO FileChannel fileChannel
	indexHeader *IndexHeader
}

func NewIndexFile(fileName string, hashSlotNum, indexNum int32, endPhyOffset, endTimestamp int64) *IndexFile {
	fileTotalSize := INDEX_HEADER_SIZE + (hashSlotNum * HASH_SLOT_SIZE) + (indexNum * INDEX_SIZE)

	indexFile := new(IndexFile)
	mapedFile, err := NewMapedFile(fileName, int64(fileTotalSize))
	if err != nil {
		// TODO
	}

	indexFile.mapedFile = mapedFile
	// TODO this.fileChannel = this.mapedFile.getFileChannel()
	indexFile.mappedByteBuffer = indexFile.mapedFile.mappedByteBuffer
	indexFile.hashSlotNum = hashSlotNum
	indexFile.indexNum = indexNum

	byteBuffer := indexFile.mappedByteBuffer.slice()
	indexFile.indexHeader = NewIndexHeader(byteBuffer)

	if endPhyOffset > 0 {
		// TODO
	}

	if endTimestamp > 0 {
		// TODO
	}

	return indexFile
}

func (self *IndexFile) load() {
	self.indexHeader.load()
}
