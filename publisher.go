package queuesgo

import "context"

type Publisher interface {
	// Send a event to the topic registered on the chosen implementation waiting for the server confirmation
	// This DO NOT wait for the server with the subscription, just the queue provider server to acknowledge the message queued
	// Returns a string with a success confirmation message according to the implementation, empty if an error occurs
	// Returns error if the given event is incorrect (Wrong payload type compared to the registered on the construction, missing metadata)
	// Returns an error if the queue provider fail to send the message
	PublishSync(ctx context.Context, event *Event) (string, error)
	// Send a event to the topic registered on the chosen implementation, without the queue provider server confirmation
	// Returns a channel that will be closed when the queue provider server finish the execution
	// The structure contained on the channel has not purpose on this version
	// Returns error if the given event is incorrect (Wrong payload type compared to the registered on the construction, missing metadata)
	PublishAsync(ctx context.Context, event *Event) (<-chan struct{}, error)
}
