package sink

import (
	"context"
	"github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
)

func SinkFactory(job *model.Job) model.MessageProcessor {
	switch job.Sink.Type {
	case "s3":
		return buildS3Sink(job)
	case "kafka":
		return buildKafkaSink(job)
	case "printer":
		return buildPrinterSink()
	default:
		panic("unknown sink type: " + job.Sink.Type)
	}

}

func buildS3Sink(job *model.Job) *S3MessageSink {
	if len(job.Sink.RoleArn) == 0 {
		panic("No role arn provided for s3 sink.")
	}
	if len(job.Sink.BucketName) == 0 {
		panic("No bucket name provided for s3 sink.")
	}

	ctx := context.TODO()
	config := aws.AssumeRole(ctx, job.Sink.RoleArn)

	sink := NewS3MessageSink(ctx, config, job.Sink.BucketName)
	return sink
}

func buildKafkaSink(job *model.Job) *KafkaMessageSink {
	if len(job.Sink.RoleArn) == 0 {
		panic("No role arn provided for kafka sink.")
	}
	if len(job.Sink.TopicName) == 0 {
		panic("No topic name provided for kafka sink.")
	}
	if len(job.Sink.Brokers) == 0 {
		panic("No brokers provided for kafka sink.")
	}

	ctx := context.TODO()
	config := aws.AssumeRole(ctx, job.Sink.RoleArn)

	return NewKafkaMessageSink(ctx, config, job.Sink.TopicName, job.Sink.Brokers)
}

func buildPrinterSink() *MessagePrinter {
	return &MessagePrinter{}
}
