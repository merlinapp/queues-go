package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	queuesgo "github.com/merlinapp/queues-go"
	"log"
	"strconv"
)

type subscriber struct {
	project          string
	subscriptionName string
	elements         []routerElement
}

type routerElement struct {
	event       string
	handlerFunc queuesgo.HandlerFunc
}

/*
Creates a new Google's pubsub subscriber implementation
the subscription name must exists already on the given projects
*/
func NewSubscriber(project, subscriptionName string) queuesgo.Subscriber {
	return &subscriber{
		project:          project,
		subscriptionName: subscriptionName,
	}
}

func (p *subscriber) RegisterFunction(eventName string, handler queuesgo.HandlerFunc) error {
	if eventName == "" {
		return errors.New("invalid event name")
	}
	p.elements = append(p.elements, routerElement{event: eventName, handlerFunc: handler})
	return nil
}

func (p *subscriber) Subscribe(ctx context.Context) error {
	pubsubClient, _ := pubsub.NewClient(ctx, p.project)
	sub := pubsubClient.Subscription(p.subscriptionName)
	err := sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		log.Printf("Received message: %s", message.Data)
		event := pubsubToEvent(message)
		ack := p.manager(ctx, event)
		if ack {
			message.Ack()
		}
	})
	return err
}

func (p *subscriber) manager(ctx context.Context, event queuesgo.Event) bool {
	eventName := event.Metadata.EventName
	for _, element := range p.elements {
		if element.event == eventName {
			ack, err := element.handlerFunc(ctx, event)
			// The acknowledgment

			if err != nil {
				log.Printf("An error: %s for event: %s", err.Error(), eventName)
				return ack
			}
			log.Printf("Operation: %s was called for event", eventName)
			return ack
		}
	}
	log.Printf("No function was registered for the event: %s", eventName)
	return true
}

func pubsubToEvent(psMessage *pubsub.Message) queuesgo.Event {
	attributes := psMessage.Attributes
	var payload map[string]interface{}
	_ = json.Unmarshal(psMessage.Data, payload)

	intTimestamp, _ := strconv.ParseInt(attributes["timestamp"], 10, 64)

	event := queuesgo.Event{
		Payload: payload,
		Metadata: queuesgo.EventMetadata{
			CorrelationID: attributes["correlation_id"],
			EventName:     attributes["event_name"],
			Origin:        attributes["origin"],
			Timestamp:     intTimestamp,
			ObjectID:      attributes["object_id"],
		},
	}
	userID, ok := attributes["user_id"]
	if ok {
		event.Metadata.UserID = userID
	}
	return event
}
