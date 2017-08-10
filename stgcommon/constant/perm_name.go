package constant

// PERM 读写权限
// @author gaoyanlei
// @since 2017/8/9
const (
	PERM_PRIORITY = 0x1 << 3
	PERM_READ     = 0x1 << 2
	PERM_WRITE    = 0x1 << 1
	PERM_INHERIT  = 0x1 << 0
)

func IsReadable(perm int32) bool {
	return (perm & PERM_READ) == PERM_READ
}

func IsWriteable(perm int32) bool {
	return (perm & PERM_WRITE) == PERM_WRITE
}

func IsInherited(perm int32) bool {
	return (perm & PERM_INHERIT) == PERM_INHERIT
}

func Perm2String(perm int32) string {
	str := "---"
	if IsReadable(perm) {
		str = "R--"
	}

	if IsWriteable(perm) {
		str = "-W-"
	}

	if IsInherited(perm) {
		str = "--X"
	}
	return str
}
