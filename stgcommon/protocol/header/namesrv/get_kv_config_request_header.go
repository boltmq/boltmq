package namesrv

// GetKVConfigRequestHeader: 创建头请求信息
// Author: yintongqiang
// Since:  2017/8/23
type GetKVConfigRequestHeader struct {
	Namespace string
	Key       string
}

func (header *GetKVConfigRequestHeader) CheckFields() error {
	return nil
}
