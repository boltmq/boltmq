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

type ConsumeMessageContext struct {
	ConsumerGroup  string
	Topic          string
	QueueId        int32
	ClientHost     string
	StoreHost      string
	MessageIds     map[string]int64
	BodyLength     int
	Success        bool
	Status         string
	MqTraceContext interface{}
}
