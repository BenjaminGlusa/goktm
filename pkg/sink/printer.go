package sink

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

type MessagePrinter struct{}

func (p *MessagePrinter) Process(message kafka.Message) error {
	println("------------------\n")
	fmt.Printf("Key: %s\n", message.Key)
	fmt.Printf("Topic: %s\n", message.Topic)
	fmt.Printf("Partition: %d\n", message.Partition)
	fmt.Printf("Offset: %d\n", message.Offset)
	fmt.Printf("Value: %s\n", message.Value)
	println("------------------\n\n")

	return nil
}
