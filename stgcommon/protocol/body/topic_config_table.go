package body

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"strings"
	"sync"
)

type TopicConfigTable struct {
	TopicConfigs map[string]*stgcommon.TopicConfig `json:"topicConfigTable"`
	sync.RWMutex `json:"-"`
}

func NewTopicConfigTable() *TopicConfigTable {
	topicConfigTable := &TopicConfigTable{
		TopicConfigs: make(map[string]*stgcommon.TopicConfig),
	}
	return topicConfigTable
}

func (table *TopicConfigTable) Size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.TopicConfigs)
}

func (self *TopicConfigTable) Keys() []string {
	self.RLock()
	defer self.RUnlock()
	if self == nil || self.TopicConfigs == nil || len(self.TopicConfigs) == 0 {
		return []string{}
	}

	topics := make([]string, 0, len(self.TopicConfigs))
	for topic, _ := range self.TopicConfigs {
		topics = append(topics, topic)
	}
	return topics
}

func (table *TopicConfigTable) Put(k string, v *stgcommon.TopicConfig) *stgcommon.TopicConfig {
	table.Lock()
	defer table.Unlock()
	table.TopicConfigs[k] = v
	return v
}

func (table *TopicConfigTable) Get(k string) *stgcommon.TopicConfig {
	table.RLock()
	defer table.RUnlock()

	v, ok := table.TopicConfigs[k]
	if !ok {
		return nil
	}

	return v
}

func (table *TopicConfigTable) Remove(k string) *stgcommon.TopicConfig {
	table.Lock()
	defer table.Unlock()

	v, ok := table.TopicConfigs[k]
	if !ok {
		return nil
	}

	delete(table.TopicConfigs, k)
	return v
}

func (table *TopicConfigTable) Foreach(fn func(k string, v *stgcommon.TopicConfig)) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range table.TopicConfigs {
		fn(k, v)
	}
}

// Clear 清空map
// author rongzhihong
// since 2017/9/18
func (table *TopicConfigTable) Clear() {
	table.Lock()
	defer table.Unlock()
	table.TopicConfigs = make(map[string]*stgcommon.TopicConfig)
}

// PutAll put all
// author rongzhihong
// since 2017/9/18
func (table *TopicConfigTable) PutAll(topicConfigTable map[string]*stgcommon.TopicConfig) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range topicConfigTable {
		table.TopicConfigs[k] = v
	}
}

// ToString 打印TopicConfigTable结构体的数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/3
func (self *TopicConfigTable) ToString() string {
	if self == nil || self.TopicConfigs == nil || self.Size() == 0 {
		return ""
	}

	self.RLock()
	defer self.RUnlock()

	infos := make([]string, 0, self.Size())
	for topic, topicConfig := range self.TopicConfigs {
		info := fmt.Sprintf("[topic=%s, %s]", topic, topicConfig.ToString())
		infos = append(infos, info)
	}

	return fmt.Sprintf("TopicConfigTable [%s]", strings.Join(infos, ","))
}
