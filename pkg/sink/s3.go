package sink

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"strings"
)

type S3ObjectPutter interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type S3MessageSink struct {
	Context    context.Context
	S3Client   S3ObjectPutter
	BucketName string
}

func (s *S3MessageSink) Upload(message model.Message) error {

	input := s3.PutObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(message.Id),
		Body:   strings.NewReader(message.Text),
	}

	_, err := s.S3Client.PutObject(s.Context, &input)

	return err
}

func (s *S3MessageSink) Process(message model.Message) error {
	return s.Upload(message)
}

func NewS3MessageSink(ctx context.Context, config aws.Config, bucketName string) *S3MessageSink {

	s3Client := s3.NewFromConfig(config)

	return &S3MessageSink{
		Context:    ctx,
		S3Client:   s3Client,
		BucketName: bucketName,
	}
}
