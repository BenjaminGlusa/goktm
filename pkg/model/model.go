package model

import "github.com/segmentio/kafka-go"

type MessageProcessor interface {
	Process(message kafka.Message) error
}

type CliOptions struct {
	Operation  string
	RoleArn    string
	Brokers    string
	TopicName  string
	GroupId    string
	BucketName string
}
