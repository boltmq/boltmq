// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package client

import (
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/boltmq/common/logger"
)

const (
	WS_DOMAIN_NAME = "env.site.net"
)

// TOPAddr 寻址服务
// Author gaoyanlei
// Since 2017/8/9
type TOPAddr struct {
	nsAddr string
	wsAddr string
}

// NewTOPAddr TOPAddr
// Author gaoyanlei
// Since 2017/8/9
func NewTOPAddr(addr string) *TOPAddr {
	return &TOPAddr{
		wsAddr: addr,
	}
}

func (taddr *TOPAddr) FetchNSAddr() string {
	return taddr.fetchNameServerAddr(true, 3000)
}

func (taddr *TOPAddr) fetchNameServerAddr(verbose bool, timeoutMills int64) string {
	resp, err := http.Get(taddr.wsAddr)
	if err != nil {
		if verbose {
			logger.Warnf("fetchZKAddr exception %s.", err)
		}
		return ""
	}

	if resp == nil {
		logger.Warn("fetch name server address failed, the response is nil.")
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		logger.Warnf("fetch name server address failed. statusCode = %d.", resp.StatusCode)
		return ""
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("fetch nameserver address err: %s.", err)
		return ""
	}

	if body == nil || len(body) == 0 {
		logger.Error("fetch nameserver address is null.")
		return ""
	}

	if verbose {
		logger.Infof("connect to %s failed, maybe the domain name %s not bind in /etc/hosts.",
			taddr.wsAddr, WS_DOMAIN_NAME)
	}

	return taddr.clearNewLine(string(body))
}

func (taddr *TOPAddr) clearNewLine(str string) string {
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
