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
package core

import (
	"net"
	"strconv"
)

type SocketAddr struct {
	IP   [net.IPv4len]byte
	Port int
}

func (sa *SocketAddr) String() string {
	return net.JoinHostPort(bytesToIPv4String(sa.IP[:]), strconv.Itoa(sa.Port))
}

func AddrToSocketAddr(addr string) SocketAddr {
	var sa SocketAddr

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return sa
	}

	sa.Port, err = strconv.Atoi(port)
	if err != nil {
		return sa
	}

	ip := ipv4StringToBytes(host)
	copy(sa.IP[:], ip[0:net.IPv4len])

	return sa
}

// IPv4 address a.b.c.d src is BigEndian buffer
func bytesToIPv4String(src []byte) string {
	return net.IPv4(src[0], src[1], src[2], src[3]).String()
}

// IPv4 address string a.b.c.d return ip bytes
func ipv4StringToBytes(host string) []byte {
	if host == "" {
		return []byte{0, 0, 0, 0}
	}

	ip := net.ParseIP(host)
	ipBytes := []byte(ip)
	return ipBytes[12:]
}
