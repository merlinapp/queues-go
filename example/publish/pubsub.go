package main

import (
	"context"
	"fmt"
	queuesgo "github.com/merlinapp/queues-go"
	"github.com/merlinapp/queues-go/pubsub"
	"os"
	"time"
)

type Book struct {
	ID       string
	AuthorID string
	Status   string
	Pages    int
}

func main() {
	pub := pubsub.NewPublisher(os.Getenv("PROJECT"), "book", &Book{})

	book := &Book{
		ID:       "test-id",
		AuthorID: "test-author-id",
		Status:   "active",
		Pages:    4,
	}

	eventCreate := &queuesgo.Event{
		Payload: book,
		Metadata: queuesgo.EventMetadata{
			UserID:        "user-test-id",
			CorrelationID: "correlation-test-id",
			EventName:     "create",
			Origin:        "books",
			Timestamp:     time.Now().Unix(),
			ObjectID:      book.ID,
		},
	}
	resultChan, err := pub.PublishAsync(context.Background(), eventCreate)
	if err != nil {
		panic("something went really wrong")
	}
	doOtherStuff()
	result := <-resultChan
	if result.Err != nil {
		fmt.Printf("something went wrong %s", result.Err.Error())
	}
	fmt.Println(result.Result)

	book.Status = "inactive"
	eventInactive := &queuesgo.Event{
		Payload: book,
		Metadata: queuesgo.EventMetadata{
			UserID:        "user-test-id",
			CorrelationID: "correlation-test-id",
			EventName:     "inactive",
			Origin:        "books",
			Timestamp:     time.Now().Unix(),
			ObjectID:      book.ID,
		},
	}

	syncResult, err := pub.PublishSync(context.Background(), eventInactive)
	if err != nil {
		fmt.Printf("something went wrong %s", result.Err.Error())
	}
	fmt.Println(syncResult)

}

func doOtherStuff() {
	fmt.Println("doing other stuff")
}
