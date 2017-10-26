package namesrv

import (
	"strings"
	"fmt"
)

// GetKVConfigRequestHeader: 创建头请求信息
// Author: yintongqiang
// Since:  2017/8/23
type GetKVConfigRequestHeader struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

func (header *GetKVConfigRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.Key) == "" {
		return fmt.Errorf("GetKVConfigRequestHeader.Key is empty")
	}
	if strings.TrimSpace(header.Namespace) == "" {
		return fmt.Errorf("GetKVConfigRequestHeader.Namespace is empty")
	}
	return nil
}
