package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"github.com/go-errors/errors"
)

// MessageExt 消息拓展结构体
type MessageExt struct {
	Message                          // 消息结构体
	QueueId                   int32  // 队列ID<PUT>
	StoreSize                 int32  // 存储记录大小
	QueueOffset               int64  // 队列偏移量
	SysFlag                   int32  // 消息标志位 <PUT>
	BornTimestamp             int64  // 消息在客户端创建时间戳 <PUT>
	BornHost                  string // 消息来自哪里 <PUT>
	StoreTimestamp            int64  // 消息在服务器存储时间戳
	StoreHost                 string // 消息存储在哪个服务器 <PUT>
	MsgId                     string // 消息ID
	CommitLogOffset           int64  // 消息对应的Commit Log Offset
	BodyCRC                   int32  // 消息体CRC
	ReconsumeTimes            int32  // 当前消息被某个订阅组重新消费了几次（订阅组之间独立计数）
	PreparedTransactionOffset int64  // 事务预处理偏移量
}

// ToString 打印消息Message的数据
func (m *MessageExt) ToString() string {
	format := "MessageExt {topic=%s, msgId=%s, storeHost=%s, bornHost=%s, storeTimestamp=%d, bornTimestamp=%d, queueId=%d, queueOffset=%d, msgBody=%s }"
	msgInfo := fmt.Sprintf(format, m.Topic, m.MsgId, m.StoreHost, m.BornHost, m.StoreTimestamp, m.BornTimestamp, m.QueueId, m.QueueOffset, string(m.Body))
	return msgInfo
}

// Encode 编码MessageExt
func (msgExt *MessageExt) Encode() ([]byte, error) {
	var (
		buf        = bytes.NewBuffer([]byte{})
		magicCode  int32
		bodyLength int32
		newBody    []byte
		e          error
	)

	// 1 TOTALSIZE
	e = binary.Write(buf, binary.BigEndian, &msgExt.StoreSize)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 2 MAGICCODE
	magicCode = int32(MessageMagicCode)
	e = binary.Write(buf, binary.BigEndian, &magicCode)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 3 BODYCRC
	e = binary.Write(buf, binary.BigEndian, &msgExt.BodyCRC)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 4 QUEUEID
	e = binary.Write(buf, binary.BigEndian, &msgExt.QueueId)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 5 FLAG
	e = binary.Write(buf, binary.BigEndian, &msgExt.Flag)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 6 QUEUEOFFSET
	e = binary.Write(buf, binary.BigEndian, &msgExt.QueueOffset)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 7 PHYSICALOFFSET
	e = binary.Write(buf, binary.BigEndian, &msgExt.CommitLogOffset)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 8 SYSFLAG
	e = binary.Write(buf, binary.BigEndian, &msgExt.SysFlag)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 9 BORNTIMESTAMP
	e = binary.Write(buf, binary.BigEndian, &msgExt.BornTimestamp)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 10 BORNHOST
	bornHost, bornPort, e := SplitHostPort(msgExt.BornHost)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	_, e = buf.Write(ipv4StringToBytes(bornHost))
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	e = binary.Write(buf, binary.BigEndian, &bornPort)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 11 STORETIMESTAMP
	e = binary.Write(buf, binary.BigEndian, &msgExt.StoreTimestamp)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 12 STOREHOST
	storeHost, storePort, e := SplitHostPort(msgExt.StoreHost)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	_, e = buf.Write(ipv4StringToBytes(storeHost))
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	e = binary.Write(buf, binary.BigEndian, &storePort)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 13 RECONSUMETIMES
	e = binary.Write(buf, binary.BigEndian, &msgExt.ReconsumeTimes)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 14 Prepared Transaction Offset
	e = binary.Write(buf, binary.BigEndian, &msgExt.PreparedTransactionOffset)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 15 BODY
	bodyLength = int32(len(msgExt.Body))
	e = binary.Write(buf, binary.BigEndian, &bodyLength)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	if bodyLength > 0 {
		if (msgExt.SysFlag & sysflag.CompressedFlag) == sysflag.CompressedFlag {
			// 压缩报文
			newBody, e = zip(msgExt.Body)
			if e != nil {
				return nil, errors.Wrap(e, 0)
			}
		} else {
			newBody = msgExt.Body
		}

		_, e = buf.Write(newBody)
		if e != nil {
			return nil, errors.Wrap(e, 0)
		}
	}

	// 16 TOPIC
	e = binary.Write(buf, binary.BigEndian, byte(len(msgExt.Topic)))
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	_, e = buf.WriteString(msgExt.Topic)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	// 17 properties
	properties := MessageProperties2Bytes(msgExt.Properties)
	e = binary.Write(buf, binary.BigEndian, int16(len(properties)))
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}
	_, e = buf.Write(properties)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	return buf.Bytes(), nil
}

func ParseTopicFilterType(sysFlag int32) stgcommon.TopicFilterType {
	if (sysFlag & sysflag.MultiTagsFlag) == sysflag.MultiTagsFlag {
		return stgcommon.MULTI_TAG
	}

	return stgcommon.SINGLE_TAG
}
