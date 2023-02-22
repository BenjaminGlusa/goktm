package operations

import (
	"context"
	"strings"

	"github.com/BenjaminGlusa/goktm/pkg/aws"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/BenjaminGlusa/goktm/pkg/sink"
	"github.com/BenjaminGlusa/goktm/pkg/source"
)

func Backup(options *model.CliOptions) {
	ctx := context.TODO()
	config := aws.AssumeRole(ctx, options.RoleArn)

	s3Sink := sink.NewS3MessageSink(ctx, config, options.BucketName)
	kafkaSource := source.NewKafkaMessageSource(ctx, config, options.TopicName, strings.Split(options.Brokers, ","), options.GroupId)

	kafkaSource.Fetch(s3Sink)
}
