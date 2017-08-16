package sysflag

// TopicSysFlag topic 配置标识
// Author gaoyanlei
// Since 2017/8/16
const (
	FLAG_UNIT     = 0x1 << 0
	FLAG_UNIT_SUB = 0x1 << 1
)

func TopicBuildSysFlag(unit bool, hasUnitSub bool) int {
	sysFlag := 0

	if unit {
		sysFlag |= FLAG_UNIT
	}

	if hasUnitSub {
		sysFlag |= FLAG_UNIT_SUB
	}

	return sysFlag
}

func SetUnitFlag(sysFlag int) int {
	return sysFlag | FLAG_UNIT
}

func ClearUnitFlag(sysFlag int) int {
	return sysFlag & (0xFFFFFFFF ^ FLAG_UNIT)
}

func HasUnitFlag(sysFlag int) bool {
	return (sysFlag & FLAG_UNIT) == FLAG_UNIT
}

func SetUnitSubFlag(sysFlag int) int {
	return sysFlag | FLAG_UNIT_SUB
}

func ClearUnitSubFlag(sysFlag int) int {
	return sysFlag & (0xFFFFFFFF ^ FLAG_UNIT_SUB)
}

func HasUnitSubFlag(sysFlag int) bool {
	return (sysFlag & FLAG_UNIT_SUB) == FLAG_UNIT_SUB
}
