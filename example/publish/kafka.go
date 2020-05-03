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
	ID        string `json:"id"`
	EventName string `json:"eventName"`
	Platform  string `json:"platform"`
}

func main() {
	pub := kafka.NewPublisher("localhost:9092", "http://localhost:8081", "pageviews", Event{})
	var n int

	flag.IntVar(&n, "n", 1, "number")
	flag.Parse()
	for i := 0; i < n; i++ {
		fmt.Println(i)
		addMsg(pub)
	}
}

func addMsg(producer queuesgo.Publisher) {
	e := &Event{
		ID:        "test-id",
		EventName: "test-event-name",
		Platform:  "android",
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
