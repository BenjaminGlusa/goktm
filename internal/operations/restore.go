package operations

import (
	"context"
	"github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/BenjaminGlusa/goktm/pkg/sink"
	"github.com/BenjaminGlusa/goktm/pkg/source"
	"strings"
)

func Restore(options *model.CliOptions) {
	ctx := context.TODO()
	config := aws.AssumeRole(ctx, options.RoleArn)

	s3source := source.NewS3Source(ctx, config, options.BucketName, options.TopicName)
	kafkaSink := sink.NewKafkaMessageSink(ctx, config, options.TopicName, strings.Split(options.Brokers, ","))

	s3source.Fetch(kafkaSink)

}
