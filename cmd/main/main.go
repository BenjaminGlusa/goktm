package main

import (
	"flag"
	"fmt"
	"github.com/BenjaminGlusa/goktm/internal/operations"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/BenjaminGlusa/goktm/pkg/random"
)

func main() {
	fmt.Println("Hello")
	options := parseOptions()

	switch options.Operation {
	case "backup":
		operations.Backup(options)
		break
	case "restore":
		operations.Restore(options)
		break
	}

}

func parseOptions() *model.CliOptions {
	var operation string
	flag.StringVar(&operation, "operation", "backup", "backup or restore")

	var roleArn string
	flag.StringVar(&roleArn, "roleArn", "", "the iam role to use to authenticate against the brokers")

	var brokers string
	flag.StringVar(&brokers, "brokers", "", "comma separated list of kafka brokers")

	var topicName string
	flag.StringVar(&topicName, "topicName", "", "name of the topic to move")

	var groupId string
	flag.StringVar(&groupId, "groupId", "", "consumer group id")

	var bucketName string
	flag.StringVar(&bucketName, "bucketName", "", "bucket to store messages in")

	flag.Parse()

	if operation != "backup" && operation != "restore" {
		panic("operation has to be either 'backup' or 'restore'")
	}

	if len(roleArn) == 0 {
		panic("No roleArn provided")
	}

	if len(brokers) == 0 {
		panic("No brokers provided")
	}

	if len(topicName) == 0 {
		panic("No topicName provided")
	}

	if len(groupId) == 0 {
		groupId = fmt.Sprintf("goktm-%s-%s", topicName, random.GenerateString(8))
	}

	if len(bucketName) == 0 {
		panic("No bucketName provided")
	}

	return &model.CliOptions{
		Operation:  operation,
		RoleArn:    roleArn,
		Brokers:    brokers,
		TopicName:  topicName,
		GroupId:    groupId,
		BucketName: bucketName,
	}

}
