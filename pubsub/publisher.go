package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	queuesgo "github.com/merlinapp/queues-go"
)

type publisher struct {
	topic *pubsub.Topic
}

func NewPublisher(project, topic string) queuesgo.Publisher {
	pubsubClient, _ := pubsub.NewClient(context.Background(), project)
	t := pubsubClient.Topic(topic)
	return &publisher{
		topic: t,
	}
}

func (p *publisher) PublishSync(ctx context.Context, event *queuesgo.Event) (string, error) {
	message, err := eventToPubSub(event)
	if err != nil {
		return "", err
	}
	result := p.topic.Publish(ctx, message)
	return result.Get(ctx)
}

func (p *publisher) PublishAsync(ctx context.Context, event *queuesgo.Event) (<-chan struct{}, error) {
	message, err := eventToPubSub(event)
	if err != nil {
		return nil, err
	}
	result := p.topic.Publish(ctx, message)
	res := result.Ready()
	return res, nil
}

func eventToPubSub(event *queuesgo.Event) (*pubsub.Message, error) {
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
