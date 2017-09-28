package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	"github.com/go-errors/errors"
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
	configVersion int32 = mqversion.CurrentVersion
)

// RemotingCommand remoting command
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-08-22
type RemotingCommand struct {
	Code         int32               `json:"code"`
	Language     string              `json:"language"`
	Version      int32               `json:"version"`
	Opaque       int32               `json:"opaque"`
	Flag         int32               `json:"flag"`
	Remark       string              `json:"remark"`
	ExtFields    map[string]string   `json:"extFields"` // 请求拓展字段
	CustomHeader CommandCustomHeader `json:"-"`         // 修改字段类型,"CustomHeader"字段不序列化 2017/8/24 Modify by jerrylou, <gunsluo@gmail.com>
	Body         []byte              `json:"-"`         // body字段不序列化
}

// CreateResponseCommand 只有通信层内部会调用，业务不会调用
func CreateDefaultResponseCommand(customHeader ...CommandCustomHeader) *RemotingCommand {
	cmd := CreateResponseCommand(SYSTEM_ERROR, "not set any response code")
	// 设置头信息
	if customHeader != nil && len(customHeader) > 0 {
		cmd.CustomHeader = customHeader[0]
	}
	return cmd
}

// CreateResponseCommand
func CreateResponseCommand(code int32, remark string) *RemotingCommand {
	cmd := &RemotingCommand{
		Code:      code,
		Remark:    remark,
		ExtFields: make(map[string]string),
		Language:  LanguageCode(GOLANG).ToString(),
	}
	// 设置为响应报文
	cmd.MarkResponseType()
	// 设置版本信息
	cmd.setCMDVersion()

	return cmd
}

// CreateRequestCommand 创建客户端请求信息 2017/8/16 Add by yintongqiang
func CreateRequestCommand(code int32, customHeader CommandCustomHeader) *RemotingCommand {
	cmd := &RemotingCommand{
		Code:         code,
		CustomHeader: customHeader,
		ExtFields:    make(map[string]string),
		Language:     LanguageCode(GOLANG).ToString(),
	}
	cmd.Opaque = inrcOpaque() // 标识自增，请求唯一标识
	cmd.setCMDVersion()       // 设置版本信息
	return cmd
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
		rc.Version = int32(v)
	}
}

// IsOnewayRPC is oneway rpc, return bool
func (rc *RemotingCommand) IsOnewayRPC() bool {
	var bits int32
	bits = 1 << rpcOneway
	return (rc.Flag & bits) == bits
}

// MarkResponseType mark response type
func (rc *RemotingCommand) MarkResponseType() {
	var bits int32
	bits = 1 << rpcType
	rc.Flag |= bits
}

// MarkOnewayRPC mark oneway type
func (rc *RemotingCommand) MarkOnewayRPC() {
	var bits int32
	bits = 1 << rpcOneway
	rc.Flag |= bits
}

// IsResponseType is response type, return bool
func (rc *RemotingCommand) IsResponseType() bool {
	var bits int32
	bits = 1 << rpcType
	return (rc.Flag & bits) == bits
}

// EncodeHeader 编码头部
func (rc *RemotingCommand) EncodeHeader() []byte {
	var (
		length       int32 = 4
		headerLength int32
	)
	// 构建头部报文
	headerData := rc.buildHeader()
	headerLength = int32(len(headerData))
	length += headerLength

	if rc.Body != nil {
		length += int32(len(rc.Body))
	}

	buf := bytes.NewBuffer([]byte{})
	// 写入报文长度
	binary.Write(buf, binary.BigEndian, length)
	// 写入报文头部长度
	binary.Write(buf, binary.BigEndian, headerLength)
	// 写入报文头部
	buf.Write(headerData)

	return buf.Bytes()
}

func (rc *RemotingCommand) buildHeader() []byte {
	// 处理custom header
	rc.makeCustomHeaderToNet()

	// json 编码
	buf, err := ffjson.Marshal(rc)
	if err != nil {
		return nil
	}
	return buf
}

func (rc *RemotingCommand) makeCustomHeaderToNet() {
	if rc.CustomHeader == nil {
		return
	}

	extFields := encodeCommandCustomHeader(rc.CustomHeader)
	for k, v := range extFields {
		rc.ExtFields[k] = v
	}
}

// Type return remoting command type
func (rc *RemotingCommand) Type() RemotingCommandType {
	if rc.IsResponseType() {
		return RESPONSE_COMMAND
	}

	return REQUEST_COMMAND
}

func (rc *RemotingCommand) DecodeCommandCustomHeader(commandCustomHeader CommandCustomHeader) error {
	if commandCustomHeader == nil {
		return nil
	}

	if rc.ExtFields == nil {
		return nil
	}

	err := decodeCommandCustomHeader(rc.ExtFields, commandCustomHeader)
	if err != nil {
		return err
	}

	return commandCustomHeader.CheckFields()
}

// DecodeRemotingCommand 解析返回RemotingCommand
func DecodeRemotingCommand(buf *bytes.Buffer) (*RemotingCommand, error) {
	var (
		length       int32
		headerLength int32
		bodyLength   int32
		body         []byte
	)

	// step 1 读取报文长度
	if buf.Len() < 4 {
		return nil, errors.Errorf("frame length[%d] incorrect，minimal is 4", buf.Len())
	}

	err := binary.Read(buf, binary.BigEndian, &length)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// step 2 读取报文头长度
	if buf.Len() < 4 {
		return nil, errors.Errorf("frame header length[%d] incorrect，minimal is 4", buf.Len())
	}

	err = binary.Read(buf, binary.BigEndian, &headerLength)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// step 3 读取报文头数据
	if buf.Len() == 0 || buf.Len() < int(headerLength) {
		return nil, errors.Errorf("frame header data[%d] incorrect，expect[%d]", buf.Len(), headerLength)
	}

	header := make([]byte, headerLength)
	_, err = buf.Read(header)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// step 4 读取报文Body
	bodyLength = length - 4 - headerLength
	if buf.Len() < int(bodyLength) {
		return nil, errors.Errorf("frame body[%d] incorrect，expect[%d]", buf.Len(), bodyLength)
	}

	if bodyLength > 0 {
		body = make([]byte, bodyLength)
		_, err = buf.Read(body)
		if err != nil {
			return nil, errors.Wrap(err, 0)
		}
	}

	// 解码
	return decodeRemotingCommand(header, body)
}

func decodeRemotingCommand(header, body []byte) (*RemotingCommand, error) {
	cmd := &RemotingCommand{}
	cmd.ExtFields = make(map[string]string)
	err := ffjson.Unmarshal(header, cmd)
	if err != nil {
		return nil, err
	}
	cmd.Body = body
	return cmd, nil
}

// Bytes 实现Serirable接口
func (rc *RemotingCommand) Bytes() []byte {
	var (
		header []byte
		packet []byte
	)

	// 头部进行编码
	header = rc.EncodeHeader()
	if rc.Body != nil && len(rc.Body) > 0 {
		packet = append(header, rc.Body...)
	} else {
		packet = header
	}

	return packet
}

// ToString 打印RemotingCommand对象数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (rc *RemotingCommand) ToString() string {
	if rc == nil {
		return "current RemotingCommand is nil"
	}
	flagBinary := fmt.Sprintf("%b", rc.Flag)
	extFields := "{}"
	if bf, err := ffjson.Marshal(rc.ExtFields); err == nil {
		extFields = string(bf)
	}

	format := "RemotingCommand [code=%d, language=%s, version=%d, opaque=%d, flag(B)=%s, remark=%s, extFields=%s]"
	info := fmt.Sprintf(format, rc.Code, rc.Language, rc.Version, rc.Opaque, flagBinary, rc.Remark, extFields)
	return info
}

//// DecodeCommand RemotingCommand转字符串
//// Author: rongzhihong
//// Since: 2017/9/19
//func (self *RemotingCommand) DecodeCommand() []byte {
//	if bf, err := ffjson.Marshal(self); err == nil {
//		return bf
//	}
//	return nil
//}
