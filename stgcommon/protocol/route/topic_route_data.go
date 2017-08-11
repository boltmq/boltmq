/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package route

import (
	"sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"strings"
)

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}
type QueueData struct {
	BrokerName     string
	ReadQueueNums  int
	WriteQueueNums int
	Perm           int
	TopicSynFlag   int
}
type BrokerData struct {
	BrokerName      string
	BrokerAddrs     map[int]string
	BrokerAddrsLock sync.RWMutex
}

func (brokerData*BrokerData)SelectBrokerAddr() string {
	value := brokerData.BrokerAddrs[stgcommon.MASTER_ID]
	if strings.EqualFold(value, "") {
		for _, value := range brokerData.BrokerAddrs {
			return value
		}
	}
	return value
}

func (topicRouteData*TopicRouteData)CloneTopicRouteData() *TopicRouteData {
	return &TopicRouteData{
		OrderTopicConf:topicRouteData.OrderTopicConf,
		//todo 有引用问题
		QueueDatas:topicRouteData.QueueDatas,
		BrokerDatas:topicRouteData.BrokerDatas,
	}
}
