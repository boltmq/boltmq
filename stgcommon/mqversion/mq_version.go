package mqversion

const (
	CurrentVersion = V3_2_6 // TODO 每次发布版本都要修改此处版本号
)

// GetVersionDesc 通过MQ版本号查找对应的版本描述
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func GetVersionDesc(versionCode int) (versionDesc string) {
	versionTable := getVersionTable()
	if versionDesc, ok := versionTable[versionCode]; ok {
		return versionDesc
	}

	return "HigherVersion"
}

// Value2Version 获取对应MQ版本号
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func Value2Version(versionCode int32) int {
	return int(versionCode) // 可以直接使用
}
