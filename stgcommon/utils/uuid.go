package utils

import (
	"github.com/satori/go.uuid"
	"strings"
)

// UUID return uuid string
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-03-24
func UUID() string {
	// UUID layout variants.
	u1 := uuid.NewV4()
	return u1.String()
}

// CUID return replace - char form uuid
func CUID() string {
	return strings.Replace(UUID(), "-", "", -1)
}
