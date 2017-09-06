package mqversion

const (
	CurrentVersion = V3_2_6 // TODO 每次发布版本都要修改此处版本号
)

func GetVersionDesc(versionCode int) (versionDesc string) {
	versionTable := getVersionTable()
	if versionDesc, ok := versionTable[versionCode]; ok {
		return versionDesc
	}

	return "HigherVersion"
}

func Value2Version(versionCode int) int {
	return versionCode // 可以直接使用
}
