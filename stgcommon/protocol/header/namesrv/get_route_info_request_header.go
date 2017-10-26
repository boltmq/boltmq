package namesrv

import (
	"strings"
	"fmt"
)

// GetRouteInfoRequestHeader: 获取topic路由信息头
// Author: yintongqiang
// Since:  2017/8/16
type GetRouteInfoRequestHeader struct {
	Topic string
}

func (header *GetRouteInfoRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.Topic) == "" {
		return fmt.Errorf("GetRouteInfoRequestHeader.Topic is empty")
	}
	return nil
}
