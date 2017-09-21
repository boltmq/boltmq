package stgstorelog

import "testing"

func Test_sc_write_read(t *testing.T) {
	// Window Path C:\Users\Administrator\test\0000"
	// Unix Path /home/test/0000"
	filePath := GetHome() + GetPathSeparator() + "test" + GetPathSeparator() + "0000"
	writeStoreCheckpoint, err := NewStoreCheckpoint(filePath)
	if err != nil {
		t.Errorf("instantiation store checkpoint error:%s", err.Error())
	}

	writeStoreCheckpoint.physicMsgTimestamp = 0xAABB
	writeStoreCheckpoint.logicsMsgTimestamp = 0xCCDD
	writeStoreCheckpoint.flush()
	writeStoreCheckpoint.shutdown()

	readStoreCheckpoint, err := NewStoreCheckpoint(filePath)
	if err != nil {
		t.Errorf("instantiation store checkpoint error:%s", err.Error())
	}

	if writeStoreCheckpoint.physicMsgTimestamp != readStoreCheckpoint.physicMsgTimestamp {
		t.Fail()
		t.Log("physicMsgTimestamp=", readStoreCheckpoint.physicMsgTimestamp)
	}

	if writeStoreCheckpoint.logicsMsgTimestamp != readStoreCheckpoint.logicsMsgTimestamp {
		t.Fail()
		t.Log("logicsMsgTimestamp=", readStoreCheckpoint.logicsMsgTimestamp)
	}
}
