package models

type TopicType int

const (
	NORMAL_TOPIC TopicType = iota // 正常队列的Topic
	RETRY_TOPIC                   // 重试队列Topic
	DLQ_TOPIC                     // 死信队列Topic
)
