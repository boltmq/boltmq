package netm

type ContextListener interface {
	OnContextConnect(ctx Context)
	OnContextClose(ctx Context)
	OnContextError(ctx Context)
	OnContextIdle(ctx Context)
}
