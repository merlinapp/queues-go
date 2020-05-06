package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ericpubu/go-kafka-avro"
	queuesgo "github.com/merlinapp/queues-go"
	"log"
	"reflect"
)

type publisher struct {
	producer   *kafka.AvroProducer
	topic      string
	schema     string
	objectType reflect.Type
}

/*
Creates a new Kafka publisher
the objectType interface should be any of the following types, any other type will cause an error returning a nil value
1. Copy of a structure
2. Non-nil pointer to a struct of the expected type.
If the structure doesn't have json tags, the schema will follow the literal fields names.
*/
func NewPublisher(kafkaServerAddress, schemaServerAddress, topic string, objectType interface{}) queuesgo.Publisher {
	if !queuesgo.ValidateType(objectType) {
		return nil
	}
	schema := createSchema(queuesgo.GetName(objectType), queuesgo.GetFields(objectType))
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return nil
	}
	fmt.Println("Schema registered: " + string(schemaBytes))
	producer, err := kafka.NewAvroProducer([]string{kafkaServerAddress}, []string{schemaServerAddress})
	if err != nil {
		log.Printf("Could not create avro producer: %s", err)
		return nil
	}
	return &publisher{
		producer:   producer,
		topic:      topic,
		schema:     string(schemaBytes),
		objectType: reflect.TypeOf(objectType),
	}
}

func (p *publisher) PublishSync(ctx context.Context, event *queuesgo.Event) (string, error) {
	data, err := p.eventToKafka(event)
	if err != nil {
		return "", err
	}
	err = p.producer.Add(p.topic, p.schema, []byte(event.Metadata.ObjectID), data)
	return event.Metadata.ObjectID, err
}

func (p *publisher) PublishAsync(ctx context.Context, event *queuesgo.Event) (<-chan queuesgo.PublicationResult, error) {
	data, err := p.eventToKafka(event)
	if err != nil {
		return nil, err
	}
	res := make(chan queuesgo.PublicationResult, 1)
	go func() {
		err = p.producer.Add(p.topic, p.schema, []byte(event.Metadata.ObjectID), data)
		p := queuesgo.PublicationResult{Result: event.Metadata.ObjectID, Err: err}
		res <- p
		close(res)
	}()
	return res, err
}

func (p *publisher) eventToKafka(event *queuesgo.Event) ([]byte, error) {
	//TODO: Add metadata into the publication
	if !queuesgo.ValidateRegisteredType(event.Payload, p.objectType) {
		return nil, errors.New("invalid payload")
	}
	data, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, errors.New("invalid payload")
	}
	return data, nil
}
