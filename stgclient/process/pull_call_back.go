package process

// PullCallback: 拉消息回调接口
// Author: yintongqiang
// Since:  2017/8/14

type PullCallback interface {
	OnSuccess(pullResultExt *PullResultExt)
}
