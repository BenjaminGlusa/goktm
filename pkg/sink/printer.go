package sink

import (
	"fmt"
	"github.com/BenjaminGlusa/goktm/pkg/model"
)

type MessagePrinter struct{}

func (p *MessagePrinter) Process(message model.Message) error {
	topic, partition, offset := model.SplitKafkaId(message.Id)

	println("------------------\n")
	fmt.Printf("Key: %s\n", message.Id)
	fmt.Printf("Topic: %s\n", topic)
	fmt.Printf("Partition: %d\n", partition)
	fmt.Printf("Offset: %d\n", offset)
	fmt.Printf("Value: %s\n", message.Text)
	println("------------------\n\n")

	return nil
}
