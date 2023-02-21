package sink

import (
	"context"
	"fmt"
	"github.com/BenjaminGlusa/goktm/pkg/random"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
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

func (s *S3MessageSink) Upload(message kafka.Message) error {

	objectKey := fmt.Sprintf("%s_%d_%d", message.Topic, message.Partition, message.Offset)

	input := s3.PutObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(string(message.Value)),
	}

	_, err := s.S3Client.PutObject(s.Context, &input)

	return err
}

func NewS3MessageSink(ctx context.Context, roleArn string, bucketName string) *S3MessageSink {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(os.Getenv("AWS_REGION")))
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	stsClient := sts.NewFromConfig(cfg)
	tempCredentials, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String(fmt.Sprintf("goktm-%s", random.GenerateString(4))),
	})
	if err != nil {
		log.Printf("Couldn't assume role %v.\n", roleArn)
		panic(err)
	}

	assumeRoleConfig, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			*tempCredentials.Credentials.AccessKeyId,
			*tempCredentials.Credentials.SecretAccessKey,
			*tempCredentials.Credentials.SessionToken),
		),
	)
	if err != nil {
		panic(err)
	}
	s3Client := s3.NewFromConfig(assumeRoleConfig)

	return &S3MessageSink{
		Context:    ctx,
		S3Client:   s3Client,
		BucketName: bucketName,
	}
}
