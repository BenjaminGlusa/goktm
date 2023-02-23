package source

import (
	"context"
	"fmt"
	"github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/BenjaminGlusa/goktm/pkg/random"
)

func SourceFactory(job *model.Job) model.MessageSource {
	switch job.Source.Type {
	case "s3":
		return buildS3MessageSource(job)
	case "kafka":
		return buildKafkaMessageSource(job)
	default:
		panic("unknown source type: " + job.Source.Type)

	}
}

func buildS3MessageSource(job *model.Job) *S3Source {
	if len(job.Source.RoleArn) == 0 {
		panic("No role arn provided for s3 source.")
	}
	if len(job.Source.BucketName) == 0 {
		panic("No bucket name provided for s3 source.")
	}
	if len(job.Source.TopicName) == 0 {
		panic("No topic name provided for s3 source.")
	}

	ctx := context.TODO()
	config := aws.AssumeRole(ctx, job.Source.RoleArn)

	return NewS3Source(ctx, config, job.Source.BucketName, job.Source.TopicName)
}

func buildKafkaMessageSource(job *model.Job) *KafkaMessageSource {
	if len(job.Source.RoleArn) == 0 {
		panic("No role arn provided for kafka source.")
	}
	if len(job.Source.TopicName) == 0 {
		panic("No topic name provided for kafka source.")
	}
	if len(job.Source.Brokers) == 0 {
		panic("No brokers provided for kafka source.")
	}
	groupId := job.Source.GroupId
	if len(groupId) == 0 {
		groupId = fmt.Sprintf("goktm-%s-%s", job.Source.TopicName, random.GenerateString(8))
	}

	ctx := context.TODO()
	config := aws.AssumeRole(ctx, job.Source.RoleArn)

	return NewKafkaMessageSource(ctx, config, job.Source.TopicName, job.Source.Brokers, groupId)
}
