package model

import "github.com/segmentio/kafka-go"

type MessageProcessor interface {
	Process(message kafka.Message) error
}
type MessageSource interface {
	Fetch(processor MessageProcessor)
}
