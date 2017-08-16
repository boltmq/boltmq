package process

import (
	"strings"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"regexp"
	"fmt"
)

const (
	CHARACTER_MAX_LENGTH = 255
	VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$"
)

func CheckGroup(group string) error {
	if strings.EqualFold(group, "") {
		return errors.New("the specified group is blank")
	}
	if len(group) > CHARACTER_MAX_LENGTH {
		return errors.New("the specified group is longer than group max length 255.")
	}
	return nil
}

func CheckMessage(msg message.Message, defaultMQProducer DefaultMQProducer) {
	if len(msg.Body) == 0 {

	}
}

func CheckTopic(topic string) {
	if strings.EqualFold(topic, "") {
		panic("the specified topic is blank")
	}
	ok,_:=regexp.MatchString(VALID_PATTERN_STR,topic)
	if !ok{
		panic(fmt.Sprintf(
			"the specified topic[%s] contains illegal characters, allowing only %s", topic,
			VALID_PATTERN_STR))
	}
}