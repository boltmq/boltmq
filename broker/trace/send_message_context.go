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
package trace

// SendMessageContext 消息发送上下文
// Author gaoyanlei
// Since 2017/8/15
type SendMessageContext struct {
	ProducerGroup string
	Topic         string
	MsgId         string
	OriginMsgId   string
	QueueId       int32
	QueueOffset   int64
	BrokerAddr    string
	BornHost      string
	BodyLength    int
	Code          int
	ErrorMsg      string
	MsgProps      string
}

// NewSendMessageContext 初始化
// Author gaoyanlei
// Since 2017/8/15
func NewSendMessageContext() *SendMessageContext {
	return &SendMessageContext{}
}
