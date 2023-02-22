package source

import (
	"context"
	"fmt"

	"crypto/tls"
	"time"

	gaws "github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam"

	"os"
)

type KafkaMessageSource struct {
	ctx       context.Context
	dialer    *kafka.Dialer
	topicName string
	brokers   []string
	groupId   string
}

// Fetch FIXME: fetch runs forever
// Idea: create a kafka.client to list topic offset and check if message with offset was read
// See:
// - https://github.com/segmentio/kafka-go/blob/4da3b721ca38db775a5024089cdc4ba14f84e698/client_test.go#L85
// - https://github.com/segmentio/kafka-go/blob/4da3b721ca38db775a5024089cdc4ba14f84e698/listoffset.go#L79
func (k *KafkaMessageSource) Fetch(processor model.MessageProcessor) {
	fmt.Printf("Connecting to: %s \n", k.brokers)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		Topic:       k.topicName,
		Dialer:      k.dialer,
		GroupID:     k.groupId,
		StartOffset: kafka.FirstOffset, // from beginning
		MaxWait:     3 * time.Second,   // wait for at most 3 seconds before receiving new data
		MinBytes:    5,
		MaxBytes:    1e6, // 1MB
	})

	defer func(conn *kafka.Reader) {
		err := conn.Close()
		if err != nil {
			panic("could not close reader, " + err.Error())
		}
	}(reader)

	for {
		println("Now reading message...")
		msg, err := reader.ReadMessage(k.ctx)

		if err != nil {
			panic("could not read message " + err.Error())
		}

		fmt.Printf("Received message: %d : %d : %v \n", msg.Partition, msg.Offset, string(msg.Value))
		err = processor.Process(msg)
		if err != nil {
			panic("could not process message " + err.Error())
		}
	}
}

func NewKafkaMessageSource(ctx context.Context, config aws.Config, topicName string, brokers []string, groupId string) *KafkaMessageSource {
	dialer := kafkaClientDialer(ctx, config)

	return &KafkaMessageSource{
		ctx:       ctx,
		dialer:    dialer,
		topicName: topicName,
		brokers:   brokers,
		groupId:   groupId,
	}
}

func saslIamMechanism(ctx context.Context, config aws.Config) sasl.Mechanism {
	signer := gaws.NewV4Signer(ctx, config)

	return &aws_msk_iam.Mechanism{
		Signer: signer,
		Region: os.Getenv("AWS_REGION"),
	}
}

func kafkaClientDialer(ctx context.Context, config aws.Config) *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
		SASLMechanism: saslIamMechanism(ctx, config),
	}
}
