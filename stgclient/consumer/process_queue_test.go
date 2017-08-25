package consumer

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"testing"
)

func TestNewTreeMap(t *testing.T) {
	pq := NewProcessQueue()
	pq.lockConsume.Lock()
	pq.MsgTreeMap.put(4, &message.MessageExt{QueueId: 3})
	pq.MsgTreeMap.put(1, &message.MessageExt{QueueId: 0})
	pq.MsgTreeMap.put(2, &message.MessageExt{QueueId: 1})
	pq.MsgTreeMap.put(3, &message.MessageExt{QueueId: 2})

	fmt.Println(pq.MsgTreeMap.firstKey())
	msg:=pq.MsgTreeMap.remove(1)
	fmt.Println(msg.QueueId)
	fmt.Println(pq.MsgTreeMap.firstKey())
	fmt.Println(pq.MsgTreeMap.get(4).QueueId)
}
