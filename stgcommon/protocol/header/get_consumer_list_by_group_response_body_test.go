package header

import (
	"fmt"
	"testing"
)

func TestGetConsumerListByGroupResponseBody_Decode(t *testing.T) {
	request := GetConsumerListByGroupResponseBody{ConsumerIdList: []string{}}
	body := &GetConsumerListByGroupResponseBody{}
	request.ConsumerIdList = append(request.ConsumerIdList, "a")
	request.ConsumerIdList = append(request.ConsumerIdList, "b")
	request.ConsumerIdList = append(request.ConsumerIdList, "c")
	data := request.CustomEncode(request)
	err := body.CustomDecode(data, body)
	if err != nil {
		fmt.Printf("GetConsumerListByGroupResponseBody err %s\n", err.Error())
		return
	}
	fmt.Println(string(data))
}
