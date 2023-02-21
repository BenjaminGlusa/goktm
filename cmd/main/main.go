package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go/aws/credentials"
	sigv4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

func main() {
	fmt.Println("Hello")
	var roleArn string
	flag.StringVar(&roleArn, "roleArn", "", "the iam role to use to authenticate against the brokers")

	var brokers string
	flag.StringVar(&brokers, "brokers", "", "comma separated list of kafka brokers")

	var topicName string
	flag.StringVar(&topicName, "topicName", "", "name of the topic to move")

	flag.Parse()
	if len(roleArn) == 0 {
		panic("No roleArn provided")
	}

	if len(brokers) == 0 {
		panic("No brokers provided")
	}

	if len(topicName) == 0 {
		panic("No topicName provided")
	}

	ctx := context.TODO()
	creds, err := assumeRole(ctx, roleArn)
	if err != nil {
		log.Printf("assume role error, " + err.Error())
		panic("assume role error, " + err.Error())
	}

	fmt.Printf("Connecting to: %s \n", brokers)
	dialer := kafkaClientDialer(creds)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(brokers, ","),
		Topic:       topicName,
		Dialer:      dialer,
		GroupID:     fmt.Sprintf("goktm-%s", topicName),
		StartOffset: kafka.FirstOffset, // from beginning
		MaxWait:     3 * time.Second,   // wait for at most 3 seconds before receiving new data
		MinBytes:    5,
		MaxBytes:    1e6, // 1MB
	})

	defer func(conn *kafka.Reader) {
		err := conn.Close()
		if err != nil {
			log.Printf("could not close reader, " + err.Error())
			panic("could not close reader, " + err.Error())
		}
	}(reader)

	for true {
		println("Now reading message...")
		msg, err := reader.ReadMessage(ctx)

		if err != nil {
			log.Printf("could not read message " + err.Error())
			panic("could not read message " + err.Error())
		}

		fmt.Printf("Received message: %d : %d : %v", msg.Partition, msg.Offset, string(msg.Value))
	}

}

func generateRandomString(l int) string {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(65 + rand.Intn(90-65))
	}
	return string(bytes)
}

func assumeRole(ctx context.Context, roleArn string) (*credentials.Credentials, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := sts.NewFromConfig(cfg)
	assumeRoleOutput, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String(fmt.Sprintf("goktm-%s", generateRandomString(4))),
	})
	if err != nil {
		panic("assume role error, " + err.Error())
	}

	return credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     *assumeRoleOutput.Credentials.AccessKeyId,
		SecretAccessKey: *assumeRoleOutput.Credentials.SecretAccessKey,
		SessionToken:    *assumeRoleOutput.Credentials.SessionToken,
		ProviderName:    "StsCredentialsProvider",
	}), nil
}

func saslIamMechanism(creds *credentials.Credentials) sasl.Mechanism {
	value, err := creds.Get()
	if err != nil {
		panic("Cannot get credentials, " + err.Error())
	}

	return &aws_msk_iam.Mechanism{
		Signer: sigv4.NewSigner(credentials.NewStaticCredentials(
			value.AccessKeyID,
			value.SecretAccessKey,
			value.SessionToken,
		)),
		Region: os.Getenv("AWS_REGION"),
	}
}

func kafkaClientDialer(creds *credentials.Credentials) *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
		SASLMechanism: saslIamMechanism(creds),
	}
}
