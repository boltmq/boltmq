package protocol

import (
	"fmt"
	"github.com/pquerna/ffjson/ffjson"
	"testing"
)

type tmpSerializable struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	*RemotingSerializable
}

func TestEncodeAndDecode(t *testing.T) {
	var tmpEncode tmpSerializable
	tmpEncode.Name = "testName"
	tmpEncode.Age = 49
	tmpEncode.RemotingSerializable = new(RemotingSerializable)

	value, err := ffjson.Marshal(tmpEncode)
	if err != nil {
		fmt.Printf("ffjson.Marshal() erre: %s \n, tmpEncode:%#v", err.Error(), tmpEncode)
		return
	}
	fmt.Printf("ffjson.Marshal() successful --> %s\n", string(value))

	var tmpDecode tmpSerializable
	err = ffjson.Unmarshal(value, &tmpDecode)
	if err != nil {
		fmt.Printf("ffjson.Unmarshal() err: %s, buf:%#v\n", err.Error(), value)
		return
	}

	fmt.Printf("ffjson.Unmarshal() successful --> name=%s, age=%d\n\n\n", tmpDecode.Name, tmpDecode.Age)
}

func TestCustomEncodeAndDecode(t *testing.T) {
	var tmpEncode tmpSerializable
	tmpEncode.Name = "tianyuliang"
	tmpEncode.Age = 27
	tmpEncode.RemotingSerializable = new(RemotingSerializable)

	value := tmpEncode.CustomEncode(tmpEncode)
	fmt.Printf("tmpEncode.CustomEncode() successful --> %s\n", string(value))

	var tmpDecode tmpSerializable
	err := tmpDecode.CustomDecode(value, &tmpDecode)
	if err != nil {
		fmt.Printf("tmpEncode.CustomDecode() err: %s, buf:%#v\n", err.Error(), value)
		return
	}
	fmt.Printf("tmpEncode.CustomDecode() successful --> name=%s, age=%d\n\n\n", tmpDecode.Name, tmpDecode.Age)
}
