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
package store

import (
	"strconv"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
)

// MessageExtInner 存储内部使用的Message对象
// Author gaoyanlei
// Since 2017/8/16
type MessageExtInner struct {
	message.MessageExt
	PropertiesString string
	TagsCode         int64
}

func (mebi *MessageExtInner) IsWaitStoreMsgOK() bool {
	properties, ok := mebi.MessageExt.Message.Properties[message.PROPERTY_WAIT_STORE_MSG_OK]
	if !ok {
		return true
	}

	result, err := strconv.ParseBool(properties)
	if err != nil {
		logger.Warnf("message parse wait store msg properties error: %s.", err)
		return true
	}

	return result
}
