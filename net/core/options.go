package core

type Options struct {
	Host           string `json:"addr"`
	Port           int    `json:"port"`
	Keepalive      bool   `json:"keepalive"`
	ReadBufferSize int    `json:"readBufferSize"`
}
