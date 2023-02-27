package model

type Message struct {
	Id string
	Text string
}

type MessageProcessor interface {
	Process(message Message) error
}

type MessageSource interface {
	Fetch(processor MessageProcessor)
}
