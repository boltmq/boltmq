package namesrv

import (
	"fmt"
	"strings"
)

// PutKVConfigRequestHeader 向Namesrv追加KV配置-请求头
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type PutKVConfigRequestHeader struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

func (header *PutKVConfigRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.Namespace) == "" {
		return fmt.Errorf("PutKVConfigRequestHeader.Namespace is empty")
	}
	if strings.TrimSpace(header.Key) == "" {
		return fmt.Errorf("PutKVConfigRequestHeader.Key is empty")
	}
	if strings.TrimSpace(header.Value) == "" {
		return fmt.Errorf("PutKVConfigRequestHeader.Value is empty")
	}
	return nil
}
