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

// IPv4 address a.b.c.d src is BigEndian buffer
func bytesToIPv4String(src []byte) string {
	return net.IPv4(src[0], src[1], src[2], src[3]).String()
}
