package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/help/faq"
	"io/ioutil"
	"net/http"
	"strings"
)

// TopAddressing 寻址服务
// Author gaoyanlei
// Since 2017/8/9
type TopAddressing struct {
	nsAddr string
	wsAddr string
}

// NewTopAddressing TopAddressing
// Author gaoyanlei
// Since 2017/8/9
func NewTopAddressing(addr string) *TopAddressing {
	return &TopAddressing{
		wsAddr: addr,
	}
}

func (self *TopAddressing) FetchNSAddr() string {
	return self.fetchNameServerAddr(true, 3000)
}

func (self *TopAddressing) fetchNameServerAddr(verbose bool, timeoutMills int64) string {
	// how to set timeoutMills for  http get request ?
	resp, err := http.Get(self.wsAddr)
	if err != nil {
		if verbose {
			fmt.Printf("fetchZKAddr exception %s\n", err.Error())
		}
		return ""
	}
	if resp == nil {
		fmt.Printf("fetch name server address failed, the response is nil\n")
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("fetch name server address failed. statusCode = %d\n", resp.StatusCode)
		return ""
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("fetch nameserver address err: %s\n", err.Error())
		return ""
	}
	if body == nil || len(body) == 0 {
		fmt.Printf("fetch nameserver address is null\n")
		return ""
	}

	if verbose {
		format := "connect to %s failed, maybe the domain name %s not bind in /etc/hosts"
		errorMsg := fmt.Sprintf(format, self.wsAddr, stgcommon.WS_DOMAIN_NAME)
		errorMsg += faq.SuggestTodo(faq.NAME_SERVER_ADDR_NOT_EXIST_URL)
		fmt.Println(errorMsg)
	}

	return self.clearNewLine(string(body))
}

func (self *TopAddressing) clearNewLine(str string) string {
	newString := strings.TrimSpace(str)
	index := strings.Index(newString, "\r")
	if index != -1 {
		return newString[0:index]
	}

	index = strings.Index(newString, "\n")
	if index != -1 {
		return newString[0:index]
	}

	return newString
}
