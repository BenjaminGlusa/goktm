package sink

import (
	"context"
	"crypto/tls"
	"fmt"
	gaws "github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/aws/aws-sdk-go-v2/aws"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaMessageSink struct {
	context   context.Context
	config    aws.Config
	topicName string
	brokers   []string
	writer    *kafka.Writer
}

func (k *KafkaMessageSink) connect() {
	if k.writer != nil {
		return
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
		SASLMechanism: gaws.SaslIamMechanism(k.context, k.config),
	}

	writerConfig := kafka.WriterConfig{
		Brokers:  k.brokers,
		Topic:    k.topicName,
		Dialer:   dialer,
		Balancer: nil,
		Async:    false,
	}
	
	k.writer = kafka.NewWriter(writerConfig)

}

func (k *KafkaMessageSink) Process(message kafka.Message) error {
	k.connect()

	key := fmt.Sprintf("%d:%d", message.Partition, message.Offset)
	fmt.Printf("Now publishing into %s: %s\n", k.topicName, key)

	err := k.writer.WriteMessages(k.context,
		kafka.Message{
			Key:   []byte(key),
			Value: message.Value,
		},
	)
	return err
}

func NewKafkaMessageSink(context context.Context, config aws.Config, topicName string, brokers []string) *KafkaMessageSink {
	return &KafkaMessageSink{
		context:   context,
		config:    config,
		topicName: topicName,
		brokers:   brokers,
		writer:    nil,
	}
}
