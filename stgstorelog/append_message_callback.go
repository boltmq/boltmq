// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
package stgstorelog

// AppendMessageCallback 写消息回调接口
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
type AppendMessageCallback interface {
	// write MapedByteBuffer,and return How many bytes to write
	doAppend(fileFromOffset int64, mappedByteBuffer *MappedByteBuffer, maxBlank int, msg interface{}) (int)
}
