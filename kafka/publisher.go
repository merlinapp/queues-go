package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dangkaka/go-kafka-avro"
	queuesgo "github.com/merlinapp/queues-go"
	"log"
	"reflect"
	"time"
)

type publisher struct {
	producer   *kafka.AvroProducer
	topic      string
	schema     string
	objectType reflect.Type
}

type Schema struct {
	Type   string  `json:"type"`
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
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

	fieldsMap := queuesgo.GetFields(objectType)
	fields := make([]Field, 0, len(fieldsMap))
	for key, val := range fieldsMap {
		//TODO: Add support for complex types
		fields = append(fields, Field{
			Name: key,
			Type: val,
		})
	}
	schema := Schema{
		Type:   "record",
		Name:   queuesgo.GetType(objectType),
		Fields: fields,
	}
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return nil
	}
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
	key := time.Now().String()
	err = p.producer.Add(p.topic, p.schema, []byte(key), data)
	return key, err
}

func (p *publisher) PublishAsync(ctx context.Context, event *queuesgo.Event) (<-chan queuesgo.PublicationResult, error) {
	data, err := p.eventToKafka(event)
	if err != nil {
		return nil, err
	}
	res := make(chan queuesgo.PublicationResult, 1)
	go func() {
		key := time.Now().String()
		err = p.producer.Add(p.topic, p.schema, []byte(key), data)
		p := queuesgo.PublicationResult{Result: key, Err: err}
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
