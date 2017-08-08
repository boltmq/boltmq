package stgclient

import (
	"strings"
	"errors"
)

const CHARACTER_MAX_LENGTH = 255

func CheckGroup(group string) error {
	if strings.EqualFold(group, "") {
		return errors.New("the specified group is blank")
	}
	if len(group) > CHARACTER_MAX_LENGTH {
		return errors.New("the specified group is longer than group max length 255.")
	}
	return nil
}
