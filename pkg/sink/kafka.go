package sink

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaMessageSink struct{
	topicName string
	brokers []string
	writer *kafka.Writer
}

func (k *KafkaMessageSink) connect() {
	if k.writer != nil {
		return
	}

	k.writer = &kafka.Writer{
		Addr:     kafka.TCP(k.brokers...),
		Topic:   k.topicName,
		Balancer: &kafka.LeastBytes{},
	}

}

func (k *KafkaMessageSink) Process(message kafka.Message) error {
	k.connect()
	
	err := k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(fmt.Sprintf("%d:%d", message.Partition, message.Offset)),
			Value: message.Value,
		},
	)
	return err
}


func NewKafkaMessageSink(topicName string, brokers []string) *KafkaMessageSink {
	return &KafkaMessageSink{
		topicName: topicName,
		brokers: brokers,
		writer: nil,
	}
}