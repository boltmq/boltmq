package header

import (
	"testing"
	"github.com/pquerna/ffjson/ffjson"
	"fmt"
)

func TestGetConsumerListByGroupResponseBody_Decode(t *testing.T) {
	request:=GetConsumerListByGroupResponseBody{ConsumerIdList:[]string{}}
	body:=&GetConsumerListByGroupResponseBody{}
	request.ConsumerIdList=append(request.ConsumerIdList,"a")
	request.ConsumerIdList=append(request.ConsumerIdList,"b")
	request.ConsumerIdList=append(request.ConsumerIdList,"c")
	data,_:=ffjson.Marshal(request)
	body.Decode(data)
	fmt.Println(string(data))
}
