package protocol

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"

	"github.com/pquerna/ffjson/ffjson"
)

// RemotingCommand 服务器与客户端通过传递RemotingCommand来交互
// Author gaoyanlei
// Since 2017/8/15
const (
	RemotingVersionKey = "rocketmq.remoting.version"
	rpcType            = 0
	rpcOneway          = 1
)

var (
	configVersion = -1
)

// RemotingCommand remoting command
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-08-22
type RemotingCommand struct {
	//header
	Code         int                 `json:"code"`
	Language     string              `json:"language"`
	Version      int                 `json:"version"`
	Opaque       int32               `json:"opaque"`
	Flag         int                 `json:"flag"`
	Remark       string              `json:"remark"`
	ExtFields    map[string]string   `json:"extFields"`
	CustomHeader CommandCustomHeader `json:"commandCustomHeader,omitempty"` // 修改字段类型 2017/8/16 Add by yintongqiang
	//body
	Body []byte `json:"-"`
}

// CreateResponseCommand
func CreateResponseCommand() *RemotingCommand {
	return new(RemotingCommand)
}

// CreateRequestCommand 创建客户端请求信息 2017/8/16 Add by yintongqiang
func CreateRequestCommand(code int, customHeader CommandCustomHeader) *RemotingCommand {
	remotingClient := &RemotingCommand{
		Code:         code,
		CustomHeader: customHeader,
		ExtFields:    make(map[string]string),
	}
	remotingClient.setCMDVersion()

	return remotingClient
}

// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-08-22
func (rc *RemotingCommand) setCMDVersion() {
	if configVersion >= 0 {
		rc.Version = configVersion
		return
	}

	version := os.Getenv(RemotingVersionKey)
	if version == "" {
		return
	}

	v, e := strconv.Atoi(version)
	if e == nil {
		rc.Version = v
	}
}

// IsOnewayRPC is oneway rpc, return bool
func (rc *RemotingCommand) IsOnewayRPC() bool {
	bits := 1 << rpcOneway
	return (rc.Flag & bits) == bits
}

// EncodeHeader 编码头部
func (rc *RemotingCommand) EncodeHeader() []byte {
	length := 4
	headerData := rc.buildHeader()
	length += len(headerData)

	if rc.Body != nil {
		length += len(rc.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, length)
	binary.Write(buf, binary.BigEndian, len(rc.Body))
	buf.Write(headerData)

	return buf.Bytes()
}

func (rc *RemotingCommand) buildHeader() []byte {
	buf, err := ffjson.Marshal(rc)
	if err != nil {
		return nil
	}
	return buf
}
