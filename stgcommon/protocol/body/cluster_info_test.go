package body

import (
	"fmt"
	"testing"
)

func TestDecodeClusterInfo(t *testing.T) {

	// {"brokerAddrTable":{"broker-a":{"brokerName":"broker-a","brokerAddrs":{"0":"10.122.2.28:10911"}}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}
	strJson := "{\"brokerAddrTable\":{\"broker-a\":{\"brokerName\":\"broker-a\",\"brokerAddrs\":{\"0\":\"10.122.2.28:10911\"}}},\"clusterAddrTable\":{\"DefaultCluster\":[\"broker-a\"]}}"

	clusterInfo := NewClusterInfo()
	err := clusterInfo.CustomDecode([]byte(strJson), clusterInfo)
	if err != nil {
		fmt.Println("\n clusterInfo ClusterAddrTable err: ", err.Error())
		fmt.Println(strJson)
	} else {
		fmt.Printf("clusterInfo.ClusterAddrTable = %s \n\n\n", clusterInfo.ClusterAddrTable)
	}

	clusterPlusInfo := NewClusterPlusInfo()
	err = clusterPlusInfo.CustomDecode([]byte(strJson), clusterPlusInfo)
	if err != nil {
		fmt.Println("clusterPlusInfo ClusterAddrTable err: ", err.Error())
		fmt.Println(strJson)
	} else {
		fmt.Printf("clusterPlusInfo.ClusterAddrTable = %s \n\n\n", clusterInfo.ClusterAddrTable)
	}

	clusterInfoV2 := clusterPlusInfo.ToClusterInfo()
	fmt.Println("clusterInfoV2: ", clusterInfoV2)
}
