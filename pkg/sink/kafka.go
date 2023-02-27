package sink

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"
	gaws "github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/aws/aws-sdk-go-v2/aws"
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

func (k *KafkaMessageSink) Process(message model.Message) error {
	k.connect()

	kafkaMessage := model.KafkaMessageFromMessage(message)
	key := fmt.Sprintf("%d:%d", kafkaMessage.Partition, kafkaMessage.Offset)
	fmt.Printf("Now publishing into %s: %s\n", k.topicName, key)

	err := k.writer.WriteMessages(k.context,
		kafka.Message{
			Key:   []byte(key),
			Value: kafkaMessage.Value,
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
