package header

import (
	"fmt"
	"github.com/pquerna/ffjson/ffjson"
	"testing"
)

func TestGetConsumerListByGroupResponseBody_Decode(t *testing.T) {
	request := GetConsumerListByGroupResponseBody{ConsumerIdList: []string{}}
	body := &GetConsumerListByGroupResponseBody{}
	request.ConsumerIdList = append(request.ConsumerIdList, "a")
	request.ConsumerIdList = append(request.ConsumerIdList, "b")
	request.ConsumerIdList = append(request.ConsumerIdList, "c")
	data, _ := ffjson.Marshal(request)
	body.Decode(data)
	fmt.Println(string(data))
}
