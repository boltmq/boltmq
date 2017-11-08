package process

import (
	"errors"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"regexp"
	"strings"
)

const (
	CHARACTER_MAX_LENGTH = 255
	VALID_PATTERN_STR    = "^[%|a-zA-Z0-9_-]+$"
)

func CheckGroup(group string) error {
	if strings.EqualFold(group, "") {
		return errors.New("the specified group is blank")
	}
	if len(group) > CHARACTER_MAX_LENGTH {
		return errors.New("the specified group is longer than group max length 255")
	}
	return nil
}

func CheckMessage(msg *message.Message, defaultMQProducer DefaultMQProducer) {
	if len(msg.Body) == 0 {
		panic("the message body length is zero")
	}
	CheckTopic(msg.Topic)
	if len(msg.Body) > defaultMQProducer.MaxMessageSize {
		format := "the message body size over max value, MAX: %v"
		panic(fmt.Sprintf(format, defaultMQProducer.MaxMessageSize))
	}
}

func CheckTopic(topic string) {
	if strings.EqualFold(topic, "") {
		panic("the specified topic is blank")
	}
	ok, _ := regexp.MatchString(VALID_PATTERN_STR, topic)
	if !ok {
		format := "the specified topic[%s] contains illegal characters, allowing only %s"
		panic(fmt.Sprintf(format, topic, VALID_PATTERN_STR))
	}
	if len([]rune(topic)) > CHARACTER_MAX_LENGTH {
		panic("the specified topic is longer than topic max length 255.")
	}
	if strings.EqualFold(stgcommon.DEFAULT_TOPIC, topic) {
		panic(fmt.Sprintf("the topic[%s] is conflict with default topic.", topic))
	}
}
