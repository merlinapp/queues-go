package queuesgo

import (
	"context"
)

type Subscriber interface {
	RegisterFunction(eventName string, handler HandlerFunc) error
	Subscribe(ctx context.Context) error
}

type HandlerFunc func(context.Context, Event) error
