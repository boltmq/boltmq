package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"strings"
)

// RegisterBrokerResult 注册broker返回结果
// Author gaoyanlei
// Since 2017/8/23
type RegisterBrokerResult struct {
	HaServerAddr string
	MasterAddr   string
	KvTable      body.KVTable
}

func (self *RegisterBrokerResult) ToString() string {
	if self == nil {
		return ""
	}
	datas := make([]string, 0, len(self.KvTable.Table))
	if self.KvTable.Table != nil {
		for key, value := range self.KvTable.Table {
			kv := fmt.Sprintf("[key=%s, value=%s]", key, value)
			datas = append(datas, kv)
		}
	}
	format := "RegisterBrokerResult[haServerAddr=%s, masterAddr=%s, kvTable=%s]"
	return fmt.Sprintf(format, self.HaServerAddr, self.MasterAddr, strings.Join(datas, ","))
}
