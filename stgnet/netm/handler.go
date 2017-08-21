package netm

import "net"

type Handler func(buffer []byte, addr string, conn net.Conn)
