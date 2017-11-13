package body

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
	"strings"
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

// ToString 打印结构信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func (self *GroupList) ToString() string {
	if self == nil || self.GroupList == nil {
		return "GroupList is nil"
	}

	var values []string
	for itor := range self.GroupList.Iterator().C {
		values = append(values, itor.(string))
	}
	return fmt.Sprintf("GroupList [%s]", strings.Join(values, ","))
}
