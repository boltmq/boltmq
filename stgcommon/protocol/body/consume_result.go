package body

type CMResult int

const (
	CR_SUCCESS CMResult = iota
	CR_LATER
	CR_ROLLBACK
	CR_COMMIT
	CR_THROW_EXCEPTION
	CR_RETURN_NULL
)

func (consumeResult CMResult) ToString() string {
	switch consumeResult {
	case CR_SUCCESS:
		return "CR_SUCCESS"
	case CR_LATER:
		return "CR_LATER"
	case CR_ROLLBACK:
		return "CR_ROLLBACK"
	case CR_COMMIT:
		return "CR_COMMIT"
	case CR_THROW_EXCEPTION:
		return "CR_THROW_EXCEPTION"
	case CR_RETURN_NULL:
		return "CR_RETURN_NULL"
	default:
		return "UnKnown"
	}
}
