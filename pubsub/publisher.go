package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	queuesgo "github.com/merlinapp/queues-go"
	"reflect"
)

type publisher struct {
	topic      *pubsub.Topic
	objectType reflect.Type
}

/*
Creates a new Google's pubsub publisher
the topic must exists already on the given projects
the objectType interface should be any of the following types, any other type will cause an error returning a nil value
1. Copy of a structure
2. No nil pointer of a structure
3. A map with key string and any value
*/
func NewPublisher(project, topic string, objectType interface{}) queuesgo.Publisher {
	pubsubClient, _ := pubsub.NewClient(context.Background(), project)
	t := pubsubClient.Topic(topic)
	if !validateType(objectType) {
		return nil
	}
	return &publisher{
		topic:      t,
		objectType: reflect.TypeOf(objectType),
	}
}

func (p *publisher) PublishSync(ctx context.Context, event *queuesgo.Event) (string, error) {
	message, err := p.eventToPubSub(event)
	if err != nil {
		return "", err
	}
	result := p.topic.Publish(ctx, message)
	return result.Get(ctx)
}

func (p *publisher) PublishAsync(ctx context.Context, event *queuesgo.Event) (<-chan struct{}, error) {
	message, err := p.eventToPubSub(event)
	if err != nil {
		return nil, err
	}
	result := p.topic.Publish(ctx, message)
	res := result.Ready()
	return res, nil
}

func (p *publisher) eventToPubSub(event *queuesgo.Event) (*pubsub.Message, error) {
	if !validateRegisteredType(event.Payload, p.objectType) {
		return nil, errors.New("invalid payload")
	}
	data, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, errors.New("invalid payload")
	}
	if event.Metadata.IsZero() {
		return nil, errors.New("invalid metadata")
	}
	attributes := make(map[string]string)
	attributes["correlation_id"] = event.Metadata.CorrelationID
	attributes["event_name"] = event.Metadata.EventName
	attributes["origin"] = event.Metadata.Origin
	attributes["object_id"] = event.Metadata.ObjectID
	attributes["timestamp"] = string(event.Metadata.Timestamp)
	if event.Metadata.UserID != "" {
		attributes["user_id"] = event.Metadata.UserID
	}
	message := &pubsub.Message{
		Attributes: attributes,
		Data:       data,
	}
	return message, nil
}
