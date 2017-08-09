package constant

import (
	"fmt"
	"testing"
)

func TestNewClientConfig(t *testing.T) {
	fmt.Println(Perm2String(PERM_READ))
	fmt.Println(Perm2String(PERM_WRITE))
	fmt.Println(Perm2String(PERM_INHERIT))
}
