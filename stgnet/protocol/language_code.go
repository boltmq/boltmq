package protocol


type LanguageCode int

const (
	JAVA LanguageCode=iota

	GOLANG
)
// 消费类型枚举
// Author: yintongqiang
// Since:  2017/8/8

func (LanguageCode LanguageCode) String() string {
	switch LanguageCode {
	case JAVA:
		return "JAVA"
	case GOLANG:
		return "GOLANG"
	default:
		return "Unknow"
	}
}