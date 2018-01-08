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

import (
	"sync"
)

// ManyPullRequest 长轮询请求
// Author rongzhihong
// Since 2017/9/5
type ManyPullRequest struct {
	pullRequestList []*PullRequest
	lock            sync.RWMutex
}

// AddPullRequest 插入请求
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) AddPullRequest(pullRequest *PullRequest) {
	req.lock.Lock()
	defer req.lock.Unlock()

	req.pullRequestList = append(req.pullRequestList, pullRequest)
}

// AddManyPullRequest 插入请求列表
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) AddManyPullRequest(mpr []*PullRequest) {
	req.lock.Lock()
	defer req.lock.Unlock()

	req.pullRequestList = append(mpr, req.pullRequestList...)
}

// CloneListAndClear 克隆并清空请求列表
// Author rongzhihong
// Since 2017/9/5
func (req *ManyPullRequest) CloneListAndClear() []*PullRequest {
	req.lock.Lock()
	defer req.lock.Unlock()

	if req.pullRequestList != nil && len(req.pullRequestList) > 0 {
		result := req.pullRequestList
		req.pullRequestList = []*PullRequest{}
		return result
	}
	return nil
}
