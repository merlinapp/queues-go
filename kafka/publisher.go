package kafka

import (
	"context"
	queuesgo "github.com/merlinapp/queues-go"
	"reflect"
)

type publisher struct {
	objectType reflect.Type
}

/*
Creates a new Kafka publisher
the objectType interface should be any of the following types, any other type will cause an error returning a nil value
1. Copy of a structure
2. Non-nil pointer to a struct of the expected type.
3. A map with key string and any value
*/
func NewPublisher(project, topic string, objectType interface{}) queuesgo.Publisher {
	if !queuesgo.ValidateType(objectType) {
		return nil
	}
	return &publisher{
		objectType: reflect.TypeOf(objectType),
	}
}

func (p *publisher) PublishSync(ctx context.Context, event *queuesgo.Event) (string, error) {
	panic("implement me")
}

func (p *publisher) PublishAsync(ctx context.Context, event *queuesgo.Event) (<-chan queuesgo.PublicationResult, error) {
	panic("implement me")
}
