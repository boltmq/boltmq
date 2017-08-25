package route

import (
	"fmt"
	"github.com/pquerna/ffjson/ffjson"
	"testing"
)

func TestTopicRouteData_Decode(t *testing.T) {
	routeData := &TopicRouteData{"abc",
		[]*QueueData{&QueueData{"test", 1, 1, 1, 1}},
		[]*BrokerData{&BrokerData{BrokerName: "test", BrokerAddrs: map[int]string{
			0: "aa",
			1: "bb",
		}}}}
	data, _ := ffjson.Marshal(routeData)
	myData:=&TopicRouteData{}
	myData.Decode(data)
	fmt.Println(string(data))
}
