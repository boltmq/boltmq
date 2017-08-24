package mqtrace

type ConsumeMessageContext struct {
	ConsumerGroup  string
	Topic          string
	QueueId        int32
	ClientHost     string
	StoreHost      string
	MessageIds     map[string]int64
	BodyLength     int
	Success        bool
	Status         string
	MqTraceContext interface{}
}
