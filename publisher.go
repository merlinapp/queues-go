package queuesgo

import "context"

type Publisher interface {
	PublishSync(ctx context.Context, event *Event) (string, error)
	PublishAsync(ctx context.Context, event *Event) (<-chan struct{}, error)
}
