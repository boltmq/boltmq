package remoting

import "github.com/boltmq/boltmq/net/core"

type ContextEventListener interface {
	OnContextIdle(ctx core.Context)
	core.EventListener
}
