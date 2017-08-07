// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache 
// license that can be found in the LICENSE file.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
package utils

import (
	"bytes"
	"encoding/binary"
)

// Int32ToBytes 整形转换成字节
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
func Int32ToBytes(n int32) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, tmp)
	return bytesBuffer.Bytes()
}

// Int32ToBytes 字节转换成整形
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return tmp
}
