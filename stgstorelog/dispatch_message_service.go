package stgstorelog

type DispatchMessageService struct {
	requestsWrite []*DispatchRequest
	requestsRead  []*DispatchRequest
	// TODO
}

func NewDispatchMessageService(putMsgIndexHightWater int32) *DispatchMessageService {
	rate := float64(putMsgIndexHightWater) * 1.5
	dms := new(DispatchMessageService)
	dms.requestsWrite = make([]*DispatchRequest, int(rate))
	dms.requestsRead = make([]*DispatchRequest, int(rate))

	return dms
}

func (self *DispatchMessageService) run() {

}
