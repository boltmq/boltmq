package header

// cloneGroupOffset 克隆cloneGroupOffset的请求头
// Author rongzhihong
// Since 2017/9/19
type CloneGroupOffsetRequestHeader struct {
	SrcGroup  string `json:"srcGroup"`
	DestGroup string `json:"destGroup"`
	Topic     string `json:"topic"`
	Offline   bool   `json:"offline"`
}

func (header *CloneGroupOffsetRequestHeader) CheckFields() error {
	return nil
}
