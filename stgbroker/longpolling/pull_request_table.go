package longpolling

import "sync"

// PullRequestTable 消息集合
// Author rongzhihong
// Since 2017/9/5
type PullRequestTable struct {
	PullRequestMap map[string]*ManyPullRequest // key:topic@queueId
	sync.RWMutex
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
	table.RLock()
	defer table.RUnlock()

	return len(table.PullRequestMap)
}

// put 存放消息
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) Put(k string, v *ManyPullRequest) {
	table.Lock()
	defer table.Unlock()
	table.PullRequestMap[k] = v
}

// get 获得key对应的value
// Author rongzhihong
// Since 2017/9/5
func (table *PullRequestTable) Get(k string) *ManyPullRequest {
	table.RLock()
	defer table.RUnlock()

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
	table.Lock()
	defer table.Unlock()

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
	table.Lock()
	defer table.Unlock()

	oldV, ok := table.PullRequestMap[k]
	if !ok {
		table.PullRequestMap[k] = v
		return nil
	}
	return oldV
}
