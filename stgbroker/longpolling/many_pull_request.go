package longpolling

import (
	"sync"
)

// ManyPullRequest 长轮询请求
// Author rongzhihong
// Since 2017/9/5
type ManyPullRequest struct {
	pullRequestList []*PullRequest
	manyPullReqLock sync.RWMutex
}

// AddPullRequest 插入请求
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) AddPullRequest(pullRequest *PullRequest) {
	req.manyPullReqLock.Lock()
	defer req.manyPullReqLock.Unlock()
	req.pullRequestList = append(req.pullRequestList, pullRequest)
}

// AddManyPullRequest 插入请求列表
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) AddManyPullRequest(mpr []*PullRequest) {
	req.manyPullReqLock.Lock()
	defer req.manyPullReqLock.Unlock()
	for _, item := range mpr {
		req.pullRequestList = append(req.pullRequestList, item)
	}

}

// CloneListAndClear 克隆并清空请求列表
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) CloneListAndClear() []*PullRequest {
	req.manyPullReqLock.Lock()
	defer req.manyPullReqLock.Unlock()

	if req.pullRequestList != nil && len(req.pullRequestList) > 0 {
		result := make([]*PullRequest, len(req.pullRequestList))
		copy(result, req.pullRequestList)
		req.pullRequestList = []*PullRequest{}
		return result
	}
	return nil
}
