package namesrv

import (
	"fmt"
	"strings"
)

// DeleteTopicInNamesrvRequestHeader 删除Topic-请求头
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type DeleteTopicInNamesrvRequestHeader struct {
	Topic string
}

func (header *DeleteTopicInNamesrvRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.Topic) == "" {
		return fmt.Errorf("DeleteTopicInNamesrvRequestHeader.Topic is empty")
	}
	return nil
}
