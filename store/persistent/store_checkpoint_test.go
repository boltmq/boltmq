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
	"testing"
)

func TestSCWriteAndRead(t *testing.T) {
	filePath := "./test/0000"
	wscp, err := newStoreCheckpoint(filePath)
	if err != nil {
		t.Errorf("instantiation store checkpoint error:%s", err.Error())
		return
	}

	wscp.physicMsgTimestamp = 0xAABB
	wscp.logicsMsgTimestamp = 0xCCDD
	wscp.flush()
	wscp.shutdown()

	rscp, err := newStoreCheckpoint(filePath)
	if err != nil {
		t.Errorf("instantiation store checkpoint error:%s", err.Error())
		return
	}

	if wscp.physicMsgTimestamp != rscp.physicMsgTimestamp {
		t.Errorf("physicMsgTimestamp=", rscp.physicMsgTimestamp)
		return
	}

	if wscp.logicsMsgTimestamp != rscp.logicsMsgTimestamp {
		t.Errorf("logicsMsgTimestamp=", rscp.logicsMsgTimestamp)
		return
	}

	if err := os.RemoveAll("./test"); err != nil {
		t.Fail()
	}
}
