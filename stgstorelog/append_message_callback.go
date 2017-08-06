// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache 
// license that can be found in the LICENSE file.
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
