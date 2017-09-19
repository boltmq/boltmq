package netm

type Options struct {
	Host string `json:"addr"`
	Port int    `json:"port"`
	Idle int    `json:"idle"`
}
