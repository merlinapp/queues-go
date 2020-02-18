package queuesgo

import (
	"encoding/json"
)

type Event struct {
	Payload  map[string]interface{} `json:"payload"`  // JSON body of the actual message
	Metadata EventMetadata          `json:"metadata"` // Event info
}

type EventMetadata struct {
	UserID        string `json:"user_id"`        // Id of the user triggering the event
	CorrelationID string `json:"correlation_id"` // Unique ID of the event, generated as is triggered the first time
	EventName     string `json:"event_name"`     // Event name (Shouldn't include origin or destination as is implicit on the topic/subscription and the extra origin field)
	Origin        string `json:"origin"`         // Service originating the event
	Timestamp     int64  `json:"timestamp"`      // Moment of the event generation (epoch millis)
	ObjectID      string `json:"object_id"`      // ID of the object changing on the event
}

func (em *EventMetadata) IsZero() bool {
	return em.CorrelationID == "" || em.EventName == "" || em.Timestamp == 0 || em.ObjectID == "" || em.Origin == ""
}

func (t Event) String() string {
	payload, _ := json.Marshal(t)
	return string(payload)
}
