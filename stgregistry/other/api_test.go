package other

import (
	"log"
	"testing"
)

func TestEtcdApi_RegisterBrokerResult(t *testing.T) {
	etcdApi := NewEtcdApi()
	etcdApi.RegisterBrokerResult(&Broker{brokerId: 1, brokerAddr: "192.168.1.1"})
	etcdApi.RegisterBrokerResult(&Broker{brokerId: 2, brokerAddr: "192.168.1.1"})
	TestEtcdApi_AllBroker(&testing.T{})
}

func TestEtcdApi_AllBroker(t *testing.T) {
	etcdApi := NewEtcdApi()
	brokerList := etcdApi.AllBroker()
	log.Printf("[brokerList]: %d\n", brokerList)
}
