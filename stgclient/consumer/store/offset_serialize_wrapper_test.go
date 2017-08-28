package store

import (
	"testing"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"github.com/pquerna/ffjson/ffjson"
	"strconv"
)



func TestNewOffsetSerializeWrapper(t *testing.T) {
	wrapper:=NewOffsetSerializeWrapper()
	mq:=message.MessageQueue{"test","master1",1}
	mExt:=MessageQueueExt{MessageQueue:mq,Offset:19}
	key:=mq.Topic+"@"+mq.BrokerName+"@"+strconv.Itoa(mq.QueueId)
	wrapper.OffsetTable[key]=mExt
	wrapper.OffsetTable[key+"1"]=mExt
	wrapper.OffsetTable[key+"2"]=mExt
	data,err:=ffjson.Marshal(wrapper)
	if err!=nil {
		fmt.Println(err)
	}else{
		fmt.Println(string(data))
	}
	newWrapper:=NewOffsetSerializeWrapper()
	ffjson.Unmarshal(data,newWrapper)
	fmt.Println(len(newWrapper.OffsetTable))
}
