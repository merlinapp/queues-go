package queuesgo

import (
	"context"
)

type Subscriber interface {
	/*
		Register the function to ve available on the subscription
		eventName: event that needs to be handled
		handler: the handler function that will be called when an event with the given name is the eventName given
	*/
	RegisterFunction(eventName string, handler HandlerFunc) error
	/*
		Blocks the current go routine to wait for events on the subscription name given on the chosen implementation
	*/
	Subscribe(ctx context.Context) error
}

/*
Function that will handle the Event received, it should return if the message should be acknowledge to the queue
provider, true to ack, false to indicate a eventual resend (The resend will depend of the subscriber implementation)
The event payload will contain the information of the registered object type (If the registered type wasn't a pointer, it will return a pointer)
*/
type HandlerFunc func(context.Context, Event) (bool, error)
