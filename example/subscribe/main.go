package main

import (
	"context"
	"fmt"
	queuesgo "github.com/merlinapp/queues-go"
	"github.com/merlinapp/queues-go/pubsub"
	"os"
)

type BookReplica struct {
	ID       string
	AuthorID string
	Status   string
}

func main() {
	sub := pubsub.NewSubscriber(os.Getenv("PROJECT"), "books-replica", &BookReplica{})
	_ = sub.RegisterFunction("create", handleBookCreation)
	_ = sub.RegisterFunction("inactive", handleBookInactivation)

	err := sub.Subscribe(context.Background())
	if err != nil {
		fmt.Println(err)
		panic("something went really wrong")
	}
}

func handleBookCreation(ctx context.Context, event queuesgo.Event) (bool, error) {
	br := event.Payload.(*BookReplica)
	saveReplica(ctx, br)
	return true, nil
}

func handleBookInactivation(ctx context.Context, event queuesgo.Event) (bool, error) {
	br := event.Payload.(*BookReplica)
	saveReplica(ctx, br)
	return false, nil
}

func saveReplica(ctx context.Context, book *BookReplica) {
	fmt.Printf("saving book replica %s", book.ID)
}
