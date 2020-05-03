package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	queuesgo "github.com/merlinapp/queues-go"
	"log"
	"reflect"
	"strconv"
)

type subscriber struct {
	project          string
	subscriptionName string
	elements         []routerElement
	objectType       reflect.Type
	logMode          bool
}

type routerElement struct {
	event       string
	handlerFunc queuesgo.HandlerFunc
}

/*
Creates a new Google's pubsub subscriber implementation
the subscription name must exists already on the given projects
the objectType interface should be any of the following types, any other type will cause an error returning a nil value
1. Copy of a structure
2. Non-nil pointer to a struct of the expected type.
3. A map with key string and any value
*/
func NewSubscriber(project, subscriptionName string, objectType interface{}, logMode bool) queuesgo.Subscriber {
	if !queuesgo.ValidateType(objectType) {
		return nil
	}
	return &subscriber{
		project:          project,
		subscriptionName: subscriptionName,
		objectType:       reflect.TypeOf(objectType),
		logMode:          logMode,
	}
}

func (s *subscriber) RegisterFunction(eventName string, handler queuesgo.HandlerFunc) error {
	if eventName == "" {
		return errors.New("invalid event name")
	}
	s.elements = append(s.elements, routerElement{event: eventName, handlerFunc: handler})
	return nil
}

func (s *subscriber) Subscribe(ctx context.Context) error {
	pubsubClient, _ := pubsub.NewClient(ctx, s.project)
	sub := pubsubClient.Subscription(s.subscriptionName)
	err := sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		s.logger(fmt.Sprintf("Received message: %s", message.Data))
		event := s.pubsubToEvent(message)
		ack := s.manager(ctx, event)
		if ack {
			message.Ack()
		}
	})
	return err
}

func (s *subscriber) manager(ctx context.Context, event queuesgo.Event) bool {
	if !queuesgo.ValidateRegisteredType(event.Payload, s.objectType) {
		panic("the received event cannot be used on the registered type")
	}
	eventName := event.Metadata.EventName
	for _, element := range s.elements {
		if element.event == eventName {
			ack, err := element.handlerFunc(ctx, event)
			// The acknowledgment of the message is handled by the handlerFunction regardless of the error
			if err != nil {
				log.Println(fmt.Sprintf("An error: %s for event: %s", err.Error(), eventName))
				return ack
			}
			s.logger(fmt.Sprintf("Operation: %s was called for event", eventName))
			return ack
		}
	}
	log.Printf("No function was registered for the event: %s", eventName)
	return true
}

func (s *subscriber) pubsubToEvent(psMessage *pubsub.Message) queuesgo.Event {
	attributes := psMessage.Attributes
	intTimestamp, _ := strconv.ParseInt(attributes["timestamp"], 10, 64)

	var payload interface{}
	if s.objectType.Kind() == reflect.Ptr {
		payload = reflect.New(s.objectType.Elem()).Interface()
	} else {
		payload = reflect.New(s.objectType).Interface()
	}

	_ = json.Unmarshal(psMessage.Data, payload)

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

func (s *subscriber) logger(message string) {
	if s.logMode {
		log.Println(message)
	}
}
