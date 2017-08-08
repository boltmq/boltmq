package stgcommon

type State int

const (
	CREATE_JUST State = iota
	RUNNING
	SHUTDOWN_ALREADY
	START_FAILED
)

func (state State) String() string {
	switch state {
	case CREATE_JUST:
		return "CREATE_JUST"
	case RUNNING:
		return "RUNNING"
	case SHUTDOWN_ALREADY:
		return "SHUTDOWN_ALREADY"
	case START_FAILED:
		return "START_FAILED"
	default:
		return "Unknow"
	}
}
