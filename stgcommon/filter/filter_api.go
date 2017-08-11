package filter

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"strings"
)
// FilterAPI: filter api
// Author: yintongqiang
// Since:  2017/8/11

type FilterAPI struct {

}

func BuildSubscriptionData(consumerGroup string, topic string, subString string) heartbeat.SubscriptionData {
	subscriptionData := heartbeat.SubscriptionData{Topic:topic, SubString:subString,TagsSet:set.NewSet(),CodeSet:set.NewSet()}
	if strings.EqualFold(subString,"")|| strings.EqualFold(subString,"*"){
		subscriptionData.SubString="*"
	}else{
      tags:=strings.Split(subString,"||")
		for _,tag:=range tags{
			trimTag:=strings.TrimSpace(tag)
			if !strings.EqualFold(trimTag,""){
              subscriptionData.TagsSet.Add(trimTag)
			 //todo 处理string hashcode问题
              subscriptionData.CodeSet.Add(trimTag)
			}
		}
	}
	return subscriptionData
}