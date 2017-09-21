package body

import (
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// GroupList分组集合
// Author rongzhihong
// Since 2017/9/19
type GroupList struct {
	GroupList set.Set `json:"groupList"`
	*protocol.RemotingSerializable
}

func NewGroupList() *GroupList {
	groupList := new(GroupList)
	groupList.GroupList = set.NewSet()
	groupList.RemotingSerializable = new(protocol.RemotingSerializable)
	return groupList
}
