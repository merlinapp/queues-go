package main

import (
	"context"
	"flag"
	"fmt"
	queuesgo "github.com/merlinapp/queues-go"
	"github.com/merlinapp/queues-go/kafka"
	"time"
)

type Event struct {
	ID         string       `json:"id"`
	EventName  string       `json:"eventName"`
	Platform   string       `json:"platform"`
	Properties []Properties `json:"properties"`
}

type Properties struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	pub := kafka.NewPublisher("localhost:9092", "http://localhost:8081", "events", Event{})
	if pub == nil {
		panic("cannot create publisher")
	}
	var n int

	flag.IntVar(&n, "n", 1, "number")
	flag.Parse()
	for i := 0; i < n; i++ {
		addMsg(pub)
	}
}

func addMsg(producer queuesgo.Publisher) {
	e := &Event{
		ID:        "test-id",
		EventName: "test-event-name",
		Platform:  "android",
		Properties: []Properties{{
			Key:   "id",
			Value: "value id",
		},
			{
				Key:   "otro",
				Value: "value cosa",
			},
		},
	}

	eventCreate := &queuesgo.Event{
		Payload: e,
		Metadata: queuesgo.EventMetadata{
			UserID:        "user-test-id",
			CorrelationID: "correlation-test-id",
			EventName:     "create",
			Origin:        "books",
			Timestamp:     time.Now().Unix(),
			ObjectID:      e.ID,
		},
	}
	id, err := producer.PublishSync(context.Background(), eventCreate)
	fmt.Println(id)
	fmt.Println(err)
}
