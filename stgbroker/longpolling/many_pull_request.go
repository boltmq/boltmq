package longpolling

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"sync"
)

// ManyPullRequest 长轮询请求
// Author rongzhihong
// Since 2017/9/5
type ManyPullRequest struct {
	pullRequestList []*PullRequest
	sync.RWMutex
}

// AddPullRequest 插入请求
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) AddPullRequest(pullRequest *PullRequest) {
	req.Lock()
	defer req.Unlock()
	defer utils.RecoveredFn()

	req.pullRequestList = append(req.pullRequestList, pullRequest)
}

// AddManyPullRequest 插入请求列表
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) AddManyPullRequest(mpr []*PullRequest) {
	req.Lock()
	defer req.Unlock()
	defer utils.RecoveredFn()

	req.pullRequestList = append(mpr, req.pullRequestList...)
}

// CloneListAndClear 克隆并清空请求列表
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) CloneListAndClear() []*PullRequest {
	req.Lock()
	defer req.Unlock()
	defer utils.RecoveredFn()

	if req.pullRequestList != nil && len(req.pullRequestList) > 0 {
		result := req.pullRequestList
		req.pullRequestList = []*PullRequest{}
		return result
	}
	return nil
}
