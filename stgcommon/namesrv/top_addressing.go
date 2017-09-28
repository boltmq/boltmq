package namesrv

// TopAddressing 寻址服务
// Author gaoyanlei
// Since 2017/8/9
type TopAddressing struct {
	nsAddr string
	wsAddr string
}

// NewTopAddressing TopAddressing
// Author gaoyanlei
// Since 2017/8/9
func NewTopAddressing(wsAddr string) *TopAddressing {
	return &TopAddressing{
		wsAddr: wsAddr,
	}
}

func (self *TopAddressing) FetchNSAddr() string {
	return fetchNSAddr(true, 3000)
}

func fetchNSAddr(verbose bool, timeoutMills int64) string {

	return ""
}
