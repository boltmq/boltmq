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
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"github.com/pquerna/ffjson/ffjson"
	"strings"
	"sync"
)

type TopicRouteData struct {
	OrderTopicConf    string              `json:"orderTopicConf"`
	QueueDatas        []*QueueData        `json:"queueDatas"`
	BrokerDatas       []*BrokerData       `json:"brokerDatas"`
	FilterServerTable map[string][]string `json:"filterServerTable"`
}
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int    `json:"readQueueNums"`
	WriteQueueNums int    `json:"writeQueueNums"`
	Perm           int    `json:"perm"`
	TopicSynFlag   int    `json:"topicSynFlag"`
}
type BrokerData struct {
	BrokerName      string         `json:"brokerName"`
	BrokerAddrs     map[int]string `json:"brokerAddrs"`
	BrokerAddrsLock sync.RWMutex   `json:"-"`
}

func (topicRouteData *TopicRouteData) Decode(data []byte) error {
	return ffjson.Unmarshal(data, topicRouteData)
}

func (brokerData *BrokerData) SelectBrokerAddr() string {
	value := brokerData.BrokerAddrs[stgcommon.MASTER_ID]
	if strings.EqualFold(value, "") {
		for _, value := range brokerData.BrokerAddrs {
			return value
		}
	}
	return value
}

func (topicRouteData *TopicRouteData) CloneTopicRouteData() *TopicRouteData {
	queueDatas := []*QueueData{}
	brokerDatas := []*BrokerData{}
	for _, queueData := range topicRouteData.QueueDatas {
		queueDatas = append(queueDatas, &QueueData{BrokerName: queueData.BrokerName, Perm: queueData.Perm,
			WriteQueueNums: queueData.WriteQueueNums, ReadQueueNums: queueData.ReadQueueNums, TopicSynFlag: queueData.TopicSynFlag})
	}
	for _, brokerData := range topicRouteData.BrokerDatas {
		brokerDatas = append(brokerDatas, &BrokerData{BrokerName: brokerData.BrokerName, BrokerAddrs: brokerData.BrokerAddrs})
	}
	return &TopicRouteData{
		OrderTopicConf: topicRouteData.OrderTopicConf,
		QueueDatas:     queueDatas,
		BrokerDatas:    brokerDatas,
	}
}

func (routeData TopicRouteData) Equals(v interface{}) bool {
	if v == nil {
		return false
	}
	rData1, ok := v.(TopicRouteData)
	var rData2 *TopicRouteData
	if !ok {
		rData2, ok = v.(*TopicRouteData)
	}
	if !ok {
		return ok
	}
	if rData2 == nil {
		if !strings.EqualFold(routeData.OrderTopicConf, rData1.OrderTopicConf) {
			return false
		}
		if len(routeData.BrokerDatas) != len(rData1.BrokerDatas) {
			return false
		}
		if len(routeData.QueueDatas) != len(rData1.QueueDatas) {
			return false
		}
		var flagB bool = true
		for i := 0; i < len(routeData.BrokerDatas); i++ {
			if !routeData.BrokerDatas[i].Equals(rData1.BrokerDatas[i]) {
				flagB = false
				break
			}
		}
		if !flagB {
			return flagB
		}
		var flagQ bool = true
		for i := 0; i < len(routeData.QueueDatas); i++ {
			if !routeData.QueueDatas[i].Equals(rData1.QueueDatas[i]) {
				flagQ = false
				break
			}
		}
		if !flagQ {
			return flagQ
		}

	} else {
		if !strings.EqualFold(routeData.OrderTopicConf, rData2.OrderTopicConf) {
			return false
		}
		if len(routeData.BrokerDatas) != len(rData2.BrokerDatas) {
			return false
		}
		if len(routeData.QueueDatas) != len(rData2.QueueDatas) {
			return false
		}
		var flagB bool = true
		for i := 0; i < len(routeData.BrokerDatas); i++ {
			if !routeData.BrokerDatas[i].Equals(rData2.BrokerDatas[i]) {
				flagB = false
				break
			}
		}
		if !flagB {
			return flagB
		}
		var flagQ bool = true
		for i := 0; i < len(routeData.QueueDatas); i++ {
			if !routeData.QueueDatas[i].Equals(rData2.QueueDatas[i]) {
				flagQ = false
				break
			}
		}
		if !flagQ {
			return flagQ
		}
	}
	return true
}

func (brokerData BrokerData) Equals(v interface{}) bool {
	if v == nil {
		return false
	}
	bData1, ok := v.(BrokerData)
	var bData2 *BrokerData
	if !ok {
		bData2, ok = v.(*BrokerData)
	}
	if !ok {
		return ok
	}
	if bData2 == nil {
		if !strings.EqualFold(brokerData.BrokerName, bData1.BrokerName) {
			return false
		}
		if len(brokerData.BrokerAddrs) != len(bData1.BrokerAddrs) {
			return false
		}
		var flag bool = true
		for brokerId, brokderAddr := range brokerData.BrokerAddrs {
			if !strings.EqualFold(brokderAddr, bData1.BrokerAddrs[brokerId]) {
				flag = false
				break
			}
		}
		return flag
	} else {
		if !strings.EqualFold(brokerData.BrokerName, bData2.BrokerName) {
			return false
		}
		if len(brokerData.BrokerAddrs) != len(bData2.BrokerAddrs) {
			return false
		}
		var flag bool = true
		for brokerId, brokderAddr := range brokerData.BrokerAddrs {
			if !strings.EqualFold(brokderAddr, bData2.BrokerAddrs[brokerId]) {
				flag = false
				break
			}
		}
		return flag
	}
}

func (queueData QueueData) Equals(v interface{}) bool {
	if v == nil {
		return false
	}
	qData1, ok := v.(QueueData)
	var qData2 *QueueData
	if !ok {
		qData2, ok = v.(*QueueData)
	}
	if !ok {
		return ok
	}
	if qData2 == nil {
		if !strings.EqualFold(queueData.BrokerName, qData1.BrokerName) {
			return false
		}
		if queueData.Perm != qData1.Perm {
			return false
		}
		if queueData.WriteQueueNums != qData1.WriteQueueNums {
			return false
		}
		if queueData.ReadQueueNums != qData1.ReadQueueNums {
			return false
		}
		if queueData.TopicSynFlag != qData1.TopicSynFlag {
			return false
		}
	} else {
		if !strings.EqualFold(queueData.BrokerName, qData2.BrokerName) {
			return false
		}
		if queueData.Perm != qData2.Perm {
			return false
		}
		if queueData.WriteQueueNums != qData2.WriteQueueNums {
			return false
		}
		if queueData.ReadQueueNums != qData2.ReadQueueNums {
			return false
		}
		if queueData.TopicSynFlag != qData2.TopicSynFlag {
			return false
		}
	}
	return true
}

type QueueDatas []*QueueData

func (self QueueDatas) Less(i, j int) bool {
	iq := self[i]
	jq := self[j]

	if iq.BrokerName < jq.BrokerName {
		return true
	} else if iq.BrokerName > jq.BrokerName {
		return false
	}
	return false
}

func (self QueueDatas) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self QueueDatas) Len() int {
	return len(self)
}

type BrokerDatas []*BrokerData

func (self BrokerDatas) Less(i, j int) bool {
	iq := self[i]
	jq := self[j]

	if iq.BrokerName < jq.BrokerName {
		return true
	} else if iq.BrokerName > jq.BrokerName {
		return false
	}
	return false
}

func (self BrokerDatas) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self BrokerDatas) Len() int {
	return len(self)
}

func (self *TopicRouteData) ToString() string {
	queueDatas := ""
	if self.QueueDatas != nil && len(self.QueueDatas) > 0 {
		queueData = strings.Join(self.QueueDatas, ",")
	}

	brokerDatas := ""
	if self.BrokerDatas != nil && len(self.BrokerDatas) > 0 {
		brokerDatas = strings.Join(self.BrokerDatas, ",")
	}

	vals := make([]string, 0, len(self.FilterServerTable))
	if self.FilterServerTable != nil && len(self.FilterServerTable) > 0 {
		for brokerAddr, filterServer := range self.FilterServerTable {
			filterServerList := ""
			if filterServer != nil && len(filterServer) > 0 {
				filterServerList = strings.Join(filterServer, ",")
			}
			val := fmt.Sprintf("brokerAddr=%s, filterServer=[%s]", brokerAddr, filterServerList)
			vals = append(vals, val)
		}
	}
	filterServerTable := strings.Join(vals, ",")

	format := "TopicRouteData [orderTopicConf=%s, queueDatas=%s, brokerDatas=%s, filterServerTable=%s]"
	info := fmt.Sprintf(format, self.OrderTopicConf, queueDatas, brokerDatas, filterServerTable)
	return info
}
