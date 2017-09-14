package protocol

type LanguageCode int

const (
	JAVA LanguageCode = iota
	CPP
	DOTNET
	PYTHON
	DELPHI
	ERLANG
	RUBY
	OTHER
	HTTP
	GOLANG
)

// 消费类型枚举
// Author: yintongqiang
// Since:  2017/8/8
func (languageCode LanguageCode) String() string {
	switch languageCode {
	case JAVA:
		return "JAVA"
	case CPP:
		return "CPP"
	case DOTNET:
		return "DOTNET"
	case PYTHON:
		return "PYTHON"
	case DELPHI:
		return "DELPHI"
	case ERLANG:
		return "ERLANG"
	case RUBY:
		return "RUBY"
	case OTHER:
		return "OTHER"
	case HTTP:
		return "HTTP"
	case GOLANG:
		return "GOLANG"
	default:
		return "Unknow"
	}
}
