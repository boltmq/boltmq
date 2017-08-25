package message

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"

	"github.com/go-errors/errors"

	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
)

// MessageDecoder: 消息解码
// Author: yintongqiang
// Since:  2017/8/16
const (
	// 消息ID定长
	MSG_ID_LENGTH = 8 + 8

	// 存储记录各个字段位置
	MessageMagicCodePostion      = 4
	MessageFlagPostion           = 16
	MessagePhysicOffsetPostion   = 28
	MessageStoreTimestampPostion = 56
	charset                      = "utf-8"
	// 序列化消息属性
	NAME_VALUE_SEPARATOR = 1
	PROPERTY_SEPARATOR   = 2
)

var (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
)

// DecodeMessageId 解析messageId
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-08-23
func DecodeMessageId(msgId string) (*MessageId, error) {

	if len(msgId) != 32 {
		return nil, errors.Errorf("msgid length[%d] invalid.", len(msgId))
	}

	buf, err := hex.DecodeString(msgId)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	messageId := &MessageId{}
	ip := bytesToIPv4String(buf[0:4])
	port := binary.BigEndian.Uint32(buf[4:8])
	messageId.Address = fmt.Sprintf("%s:%d", ip, port)
	messageId.Offset = binary.BigEndian.Uint64(buf[8:16])

	return messageId, nil
}

// DecodesMessageExt 解析消息体，返回多个消息
func DecodesMessageExt(buffer []byte, isReadBody bool) ([]*MessageExt, error) {
	var (
		buf     = bytes.NewBuffer(buffer)
		msgExts []*MessageExt
	)

	for buf.Len() > 0 {
		msgExt, err := decodeMessageExt(buf, isReadBody, true)
		if err != nil {
			return nil, err
		}
		msgExts = append(msgExts, msgExt)
	}

	return msgExts, nil
}

// DecodeMessageExt 解析消息体，返回MessageExt
func DecodeMessageExt(buffer []byte, isReadBody, isCompressBody bool) (*MessageExt, error) {
	var (
		buf = bytes.NewBuffer(buffer)
	)

	return decodeMessageExt(buf, isReadBody, isCompressBody)
}

// 解析消息体，返回MessageExt
func decodeMessageExt(buf *bytes.Buffer, isReadBody, isCompressBody bool) (*MessageExt, error) {
	var (
		bornHost         = make([]byte, 4)
		bornPort         int32
		storeHost        = make([]byte, 4)
		storePort        int32
		magicCode        int32
		bodyLength       int32
		physicOffset     int64
		topicLength      byte
		propertiesLength int16
		e                error
	)

	msgExt := &MessageExt{}
	// 1 TOTALSIZE
	e = binary.Read(buf, binary.BigEndian, &msgExt.StoreSize)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 2 MAGICCODE
	e = binary.Read(buf, binary.BigEndian, &magicCode)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 3 BODYCRC
	e = binary.Read(buf, binary.BigEndian, &msgExt.BodyCRC)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 4 QUEUEID
	e = binary.Read(buf, binary.BigEndian, &msgExt.QueueId)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 5 FLAG
	e = binary.Read(buf, binary.BigEndian, &msgExt.Flag)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 6 QUEUEOFFSET
	e = binary.Read(buf, binary.BigEndian, &msgExt.QueueOffset)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 7 PHYSICALOFFSET
	e = binary.Read(buf, binary.BigEndian, &physicOffset)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 8 SYSFLAG
	e = binary.Read(buf, binary.BigEndian, &msgExt.SysFlag)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 9 BORNTIMESTAMP
	e = binary.Read(buf, binary.BigEndian, &msgExt.BornTimestamp)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 10 BORNHOST
	e = binary.Read(buf, binary.BigEndian, &bornHost)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	e = binary.Read(buf, binary.BigEndian, &bornPort)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 11 STORETIMESTAMP
	e = binary.Read(buf, binary.BigEndian, &msgExt.StoreTimestamp)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 12 STOREHOST
	e = binary.Read(buf, binary.BigEndian, &storeHost)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	e = binary.Read(buf, binary.BigEndian, &storePort)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 13 RECONSUMETIMES
	e = binary.Read(buf, binary.BigEndian, &msgExt.ReconsumeTimes)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 14 Prepared Transaction Offset
	e = binary.Read(buf, binary.BigEndian, &msgExt.PreparedTransactionOffset)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	// 15 BODY
	e = binary.Read(buf, binary.BigEndian, &bodyLength)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	if bodyLength > 0 {
		if isReadBody {
			body := make([]byte, bodyLength)
			e = binary.Read(buf, binary.BigEndian, body)
			if e != nil {
				return nil, errors.Wrap(e, 0)
			}

			// 解压缩
			if isCompressBody && (msgExt.SysFlag&sysflag.CompressedFlag) == sysflag.CompressedFlag {
				unzipBytes, e := unzip(body)
				if e != nil {
					return nil, e
				}
				msgExt.Body = unzipBytes
			} else {
				msgExt.Body = body
			}
		} else {
			buf.Next(int(bodyLength))
		}
	}

	// 16 TOPIC
	e = binary.Read(buf, binary.BigEndian, &topicLength)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	topic := make([]byte, topicLength)
	e = binary.Read(buf, binary.BigEndian, &topic)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	msgExt.Topic = string(topic)

	// 17 properties
	e = binary.Read(buf, binary.BigEndian, &propertiesLength)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	if propertiesLength > 0 {
		properties := make([]byte, propertiesLength)
		binary.Read(buf, binary.BigEndian, &properties)

		// 解析消息属性
		msgExt.Properties = Bytes2messageProperties(properties)
	}

	// 组装消息BornHost字段
	msgExt.BornHost = JoinHostPort(bornHost, bornPort)
	// 组装消息StoreHost字段
	msgExt.StoreHost = JoinHostPort(storeHost, storePort)
	msgExt.CommitLogOffset = physicOffset
	// 组装消息ID字段
	msgExt.MsgId, e = createMessageId(storeHost, storePort, physicOffset)
	if e != nil {
		return nil, e
	}

	return msgExt, nil
}

// 修复string不可见字符问题，使用[]byte  Modify: jerrylou, <gunsluo@gmail.com> Since: 2017-08-25
func MessageProperties2String(properties map[string]string) string {
	return string(MessageProperties2Bytes(properties))
}

func MessageProperties2Bytes(properties map[string]string) []byte {
	var (
		b = bytes.NewBuffer([]byte{})
	)
	for k, v := range properties {
		b.WriteString(k)
		b.WriteByte(byte(NAME_VALUE_SEPARATOR))
		b.WriteString(v)
		b.WriteByte(byte(PROPERTY_SEPARATOR))
	}
	return b.Bytes()
}

// 修复string不可见字符问题，使用[]byte  Modify: jerrylou, <gunsluo@gmail.com> Since: 2017-08-25
func String2messageProperties(propertiesStr string) map[string]string {
	return Bytes2messageProperties([]byte(propertiesStr))
}

func Bytes2messageProperties(propertiesBuf []byte) map[string]string {
	var (
		tbuf = propertiesBuf
	)
	properties := make(map[string]string)
	for len(tbuf) > 0 {
		pi := bytes.IndexByte(tbuf, PROPERTY_SEPARATOR)
		if pi == -1 {
			break
		}

		propertie := tbuf[0:pi]

		ni := bytes.IndexByte(propertie, NAME_VALUE_SEPARATOR)
		if ni == -1 || ni > pi {
			break
		}

		key := string(propertie[0:ni])
		properties[key] = string(propertie[ni+1:])

		tbuf = tbuf[pi+1:]
	}

	return properties
}

// CreateMessageId 解析消息msgId字段addr是host:port
func CreateMessageId(addr string, offset int64) (string, error) {
	host, port, e := SplitHostPort(addr)
	if e != nil {
		return "", e
	}

	return createMessageId(ipv4StringToBytes(host), port, offset)
}

// 解析消息msgId字段(ip + port + commitOffset，其中ip、port长度分别是4位，offset占用8位长度)
func createMessageId(storeHost []byte, storePort int32, offset int64) (string, error) {
	var (
		buf = bytes.NewBuffer([]byte{})
		e   error
	)

	buf.Grow(MSG_ID_LENGTH)
	_, e = buf.Write(storeHost)
	if e != nil {
		return "", errors.Wrap(e, 0)
	}

	e = binary.Write(buf, binary.BigEndian, &storePort)
	if e != nil {
		return "", errors.Wrap(e, 0)
	}

	e = binary.Write(buf, binary.BigEndian, &offset)
	if e != nil {
		return "", errors.Wrap(e, 0)
	}

	return bytesToHexString(buf.Bytes()), nil
}

// JoinHostPort 连接host:port
func JoinHostPort(hostBytes []byte, port int32) string {
	host := bytesToIPv4String(hostBytes)
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// SplitHostPort 解析host:port
func SplitHostPort(addr string) (string, int32, error) {
	if addr == "" {
		return "", 0, nil
	}

	host, portStr, e := net.SplitHostPort(addr)
	if e != nil {
		return "", 0, errors.Wrap(e, 0)
	}

	port, e := strconv.ParseInt(portStr, 10, 32)
	if e != nil {
		return host, 0, errors.Wrap(e, 0)
	}

	return host, int32(port), nil
}

// IPv4 address a.b.c.d src is BigEndian buffer
func bytesToIPv4String(src []byte) string {
	return net.IPv4(src[0], src[1], src[2], src[3]).String()
}

// IPv4 address string a.b.c.d return ip bytes
func ipv4StringToBytes(host string) []byte {
	if host == "" {
		return []byte{0, 0, 0, 0}
	}

	ip := net.ParseIP(host)
	ipBytes := []byte(ip)
	return ipBytes[12:]
}

func int32ToBytes(value int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return buf
}

func int64ToBytes(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

func bytesToHexString(src []byte) string {
	return strings.ToUpper(hex.EncodeToString(src))
}

// 压缩报文
func zip(buffer []byte) ([]byte, error) {
	var (
		b bytes.Buffer
	)

	w := zlib.NewWriter(&b)
	_, e := w.Write(buffer)
	if e != nil {
		w.Close()
		return nil, errors.Wrap(e, 0)
	}
	// don't use defer, because b.Bytes() after w.Close()
	w.Close()

	return b.Bytes(), nil
}

// 解压报文
func unzip(buffer []byte) ([]byte, error) {
	var (
		b = bytes.NewReader(buffer)
	)

	z, e := zlib.NewReader(b)
	defer z.Close()
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	unzipBytes, e := ioutil.ReadAll(z)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	return unzipBytes, nil
}
