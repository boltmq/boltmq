package namesrv

import (
	"fmt"
	"strings"
)

// DeleteKVConfigRequestHeader 删除KV配置项-请求头
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type DeleteKVConfigRequestHeader struct {
	Namespace string
	Key       string
}

func (header *DeleteKVConfigRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.Key) == "" {
		return fmt.Errorf("DeleteKVConfigRequestHeader.Key is empty")
	}
	if strings.TrimSpace(header.Namespace) == "" {
		return fmt.Errorf("DeleteKVConfigRequestHeader.Namespace is empty")
	}
	return nil
}
