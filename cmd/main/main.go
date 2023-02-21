package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/BenjaminGlusa/goktm/pkg/random"
	"github.com/BenjaminGlusa/goktm/pkg/sink"
	"github.com/BenjaminGlusa/goktm/pkg/source"
)

func main() {
	fmt.Println("Hello")
	options := parseOptions()
	
	ctx := context.TODO()
	config := aws.AssumeRole(ctx, options.RoleArn)

	s3Sink := sink.NewS3MessageSink(ctx, config, options.BucketName)
	kafkaSource := source.NewKafkaMessageSource(ctx, config, options.TopicName, strings.Split(options.Brokers, ","), options.GroupId)

	kafkaSource.Fetch(s3Sink)
	
}


func parseOptions() *model.CliOptions {

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
		RoleArn: roleArn,
		Brokers: brokers,
		TopicName: topicName,
		GroupId: groupId,
		BucketName: bucketName,
	}

}