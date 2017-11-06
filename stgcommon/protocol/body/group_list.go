package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// GroupList 分组集合
// Author rongzhihong
// Since 2017/9/19
type GroupList struct {
	GroupList set.Set `json:"groupList"`
	*protocol.RemotingSerializable
}

// NewGroupList 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewGroupList() *GroupList {
	groupList := new(GroupList)
	groupList.GroupList = set.NewSet()
	groupList.RemotingSerializable = new(protocol.RemotingSerializable)
	return groupList
}
