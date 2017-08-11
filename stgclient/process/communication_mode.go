package process

type CommunicationMode int

const (
	SYNC CommunicationMode = iota
	ASYNC
	ONEWAY
)
func ( cm CommunicationMode) String() string {
	switch cm {
	case SYNC:
		return "SYNC"
	case ASYNC:
		return "ASYNC"
	case ONEWAY:
		return "ONEWAY"
	default:
		return "Unknow"
	}
}
