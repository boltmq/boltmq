package namesrv

// TopAddressing 寻址服务
// @author gaoyanlei
// @since 2017/8/9
type TopAddressing struct {

	// TODO Logger log = LoggerFactory.getLogger(LoggerName.CommonLoggerName);
	nsAddr string
	wsAddr string
}

// NewTopAddressing TopAddressing
// @author gaoyanlei
// @since 2017/8/9
func NewTopAddressing(wsAddr string) *TopAddressing {
	return &TopAddressing{
		wsAddr: wsAddr,
	}
}
