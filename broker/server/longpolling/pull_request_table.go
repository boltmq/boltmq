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
package longpolling

import "sync"

// PullRequestTable 消息集合
// Author rongzhihong
// Since 2017/9/5
type PullRequestTable struct {
	PullRequestMap map[string]*ManyPullRequest // key:topic@queueId
	lock           sync.RWMutex
}

// newPullRequestTable 初始化消息集合
// Author rongzhihong
// Since 2017/9/5
func NewPullRequestTable() *PullRequestTable {
	return &PullRequestTable{
		PullRequestMap: make(map[string]*ManyPullRequest, 1024),
	}
}

// size 消息集合长度
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) Size() int {
	table.lock.RLock()
	defer table.lock.RUnlock()

	return len(table.PullRequestMap)
}

// put 存放消息
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) Put(k string, v *ManyPullRequest) {
	table.lock.Lock()
	defer table.lock.Unlock()
	table.PullRequestMap[k] = v
}

// get 获得key对应的value
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) Get(k string) *ManyPullRequest {
	table.lock.RLock()
	defer table.lock.RUnlock()

	v, ok := table.PullRequestMap[k]
	if !ok {
		return nil
	}

	return v
}

// remove 删除消息
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) Remove(k string) *ManyPullRequest {
	table.lock.Lock()
	defer table.lock.Unlock()

	v, ok := table.PullRequestMap[k]
	if !ok {
		return nil
	}

	delete(table.PullRequestMap, k)
	return v
}

// putIfAbsent 不存在时，才存放消息
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) PutIfAbsent(k string, v *ManyPullRequest) *ManyPullRequest {
	table.lock.Lock()
	defer table.lock.Unlock()

	old, ok := table.PullRequestMap[k]
	if !ok {
		table.PullRequestMap[k] = v
		return nil
	}
	return old
}
