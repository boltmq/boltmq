package netm

type Handler func(buffer []byte, ctx Context)
