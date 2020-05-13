package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
	queuesgo "github.com/merlinapp/queues-go"
	"log"
	"reflect"
	"strings"
)

type publisher struct {
	producer             *ckafka.Producer
	schemaRegistryClient *CachedSchemaRegistryClient
	topic                string
	schema               string
	objectType           reflect.Type
}

/*
Creates a new Kafka publisher
the kafkaServerAddresses string can receive several hosts separated by ','
the objectType interface should be any of the following types, any other type will cause an error returning a nil value
1. Copy of a structure
2. Non-nil pointer to a struct of the expected type.
If the structure doesn't have json tags, the schema will follow the literal fields names.
*/
func NewPublisher(kafkaServerHosts, schemaServerAddress, topic string, objectType interface{}) queuesgo.Publisher {
	if !queuesgo.ValidateType(objectType) {
		return nil
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(strings.Split(schemaServerAddress, ","))

	schema := createSchema(queuesgo.GetName(objectType), queuesgo.GetFields(objectType))
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return nil
	}
	fmt.Println("Schema registered: " + string(schemaBytes))

	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": kafkaServerHosts})
	if err != nil {
		log.Printf("Could not create avro producer: %s", err)
		return nil
	}
	return &publisher{
		producer:             producer,
		schemaRegistryClient: schemaRegistryClient,
		topic:                topic,
		schema:               string(schemaBytes),
		objectType:           reflect.TypeOf(objectType),
	}
}

func (p *publisher) PublishSync(ctx context.Context, event *queuesgo.Event) (string, error) {
	data, headers, err := p.eventToKafka(event)
	if err != nil {
		return "", err
	}
	return p.sendMessage([]byte(event.Metadata.ObjectID), data, headers)
}

func (p *publisher) PublishAsync(ctx context.Context, event *queuesgo.Event) (<-chan queuesgo.PublicationResult, error) {
	data, headers, err := p.eventToKafka(event)
	if err != nil {
		return nil, err
	}
	res := make(chan queuesgo.PublicationResult, 1)
	go func() {
		result, err := p.sendMessage([]byte(event.Metadata.ObjectID), data, headers)
		p := queuesgo.PublicationResult{Result: result, Err: err}
		res <- p
		close(res)
	}()
	return res, err
}

//GetSchemaId get schema id from schema-registry service
func (p *publisher) getSchemaId(avroCodec *goavro.Codec) (int, error) {
	schemaId, err := p.schemaRegistryClient.CreateSubject(p.topic+"-value", avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (p *publisher) sendMessage(key []byte, value []byte, headers []ckafka.Header) (string, error) {
	avroCodec, err := goavro.NewCodec(p.schema)
	schemaId, err := p.getSchemaId(avroCodec)
	if err != nil {
		return "", err
	}
	native, _, err := avroCodec.NativeFromTextual(value)
	if err != nil {
		return "", err
	}
	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return "", err
	}

	avrEncoder := &AvroEncoder{
		SchemaID: schemaId,
		Content:  binaryValue,
	}

	message, _ := avrEncoder.Encode()
	deliveryChan := make(chan ckafka.Event)

	err = p.producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &p.topic, Partition: ckafka.PartitionAny},
		Key:            key,
		Value:          message,
		Headers:        headers,
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*ckafka.Message)

	var msg string
	if m.TopicPartition.Error != nil {
		err = m.TopicPartition.Error
	} else {
		msg = fmt.Sprintf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(deliveryChan)
	return msg, err
}

func (p *publisher) eventToKafka(event *queuesgo.Event) ([]byte, []ckafka.Header, error) {
	if !queuesgo.ValidateRegisteredType(event.Payload, p.objectType) {
		return nil, nil, errors.New("invalid payload")
	}
	data, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, nil, errors.New("invalid payload")
	}
	headers := make([]ckafka.Header, 5)
	headers[0] = ckafka.Header{
		Key:   "correlation_id",
		Value: []byte(event.Metadata.CorrelationID),
	}
	headers[1] = ckafka.Header{
		Key:   "event_name",
		Value: []byte(event.Metadata.EventName),
	}
	headers[2] = ckafka.Header{
		Key:   "origin",
		Value: []byte(event.Metadata.Origin),
	}
	headers[3] = ckafka.Header{
		Key:   "object_id",
		Value: []byte(event.Metadata.ObjectID),
	}
	headers[4] = ckafka.Header{
		Key:   "timestamp",
		Value: []byte(string(event.Metadata.Timestamp)),
	}
	return data, headers, nil
}
