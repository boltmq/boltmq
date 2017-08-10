package client

import (
	"fmt"
	"testing"
)

func TestNewClientConfig(t *testing.T) {
	producerManager := NewProducerManager()
	for i := 0; i < 10000; i++ {
		fmt.Println(producerManager.generateRandmonNum())
	}
}
