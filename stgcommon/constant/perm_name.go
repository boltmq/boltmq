package constant

// PERM 读写权限
// Author gaoyanlei
// Since 2017/8/9
const (
	PERM_PRIORITY = 0x1 << 3
	PERM_READ     = 0x1 << 2
	PERM_WRITE    = 0x1 << 1
	PERM_INHERIT  = 0x1 << 0
)

func IsReadable(perm int) bool {
	return (perm & PERM_READ) == PERM_READ
}

func IsWriteable(perm int) bool {
	return (perm & PERM_WRITE) == PERM_WRITE
}

func IsInherited(perm int) bool {
	return (perm & PERM_INHERIT) == PERM_INHERIT
}

func Perm2String(perm int) string {
	str := "---"
	if IsReadable(perm) {
		str = "R" + str[1:]
	}

	if IsWriteable(perm) {
		str = str[:1] + "W" + str[2:]
	}

	if IsInherited(perm) {
		str = str[:2] + "X"
	}
	return str
}
