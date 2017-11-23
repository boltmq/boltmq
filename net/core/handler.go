package core

import "net"

//type Handler func(buffer []byte, ctx Context)
type Handler func(buffer []byte, ctx net.Conn)
