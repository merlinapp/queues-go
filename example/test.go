package main

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

}
