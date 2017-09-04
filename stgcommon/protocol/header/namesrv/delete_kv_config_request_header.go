package namesrv

// DeleteKVConfigRequestHeader 删除KV配置项-请求头
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type DeleteKVConfigRequestHeader struct {
	Namespace string
	Key       string
}

func (header *DeleteKVConfigRequestHeader) CheckFields() error {
	return nil
}
