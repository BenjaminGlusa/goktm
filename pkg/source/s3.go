package source

import (
	"context"
	"fmt"
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/segmentio/kafka-go"
	"io"
	"sort"
	"strconv"
	"strings"
)

type S3ObjectGetter interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type S3Source struct {
	Context    context.Context
	S3Client   S3ObjectGetter
	BucketName string
	TopicName  string
}

func (s *S3Source) Fetch(processor model.MessageProcessor) {
	fileNames := s.ListFiles(nil)
	fileNames = sortFileNames(fileNames)

	totalMessages := len(fileNames)
	fmt.Printf("There are %d messages to process.\n", totalMessages)

	for idx, fileName := range fileNames {
		fmt.Printf("Processing file %d/%d\n", idx+1, totalMessages)

		content := s.GetFileContent(fileName)
		message := s.CreateMessage(fileName, content)

		err := processor.Process(message)
		if err != nil {
			panic("Could not process message: " + err.Error())
		}
	}
	println("All messages processed.")
}

func sortFileNames(fileNames []string) []string {
	sort.Slice(fileNames, func(a, b int) bool {
		partsA := strings.Split(fileNames[a], "_")
		partsB := strings.Split(fileNames[b], "_")

		//  compare offsets if partitions are equal
		if partsA[1] == partsB[1] {
			return partsA[2] < partsB[2]
		}
		// compare partitions
		return partsA[1] < partsB[1]
	})
	return fileNames
}

// ListFiles
// Note: continuationToken is used for recursion. Set it to `nil` for
// the first call
func (s *S3Source) ListFiles(continuationToken *string) []string {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.BucketName),
		Prefix: aws.String(s.TopicName),
	}

	if continuationToken != nil {
		params.ContinuationToken = continuationToken
	}

	response, err := s.S3Client.ListObjectsV2(s.Context, params)
	if err != nil {
		panic("could not list files: " + err.Error())
	}

	keys := make([]string, len(response.Contents))
	for idx, obj := range response.Contents {
		keys[idx] = *obj.Key
	}

	if response.IsTruncated {
		// get next page via recursion
		nextPage := s.ListFiles(response.ContinuationToken)
		return append(keys, nextPage...)
	}

	return keys
}

func (s *S3Source) GetFileContent(fileName string) []byte {
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(fileName),
	}

	result, err := s.S3Client.GetObject(s.Context, params)
	if err != nil {
		panic("Could not download file: " + err.Error())
	}

	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		panic("Could not read file: " + err.Error())
	}

	return body
}

func (s *S3Source) CreateMessage(fileName string, content []byte) kafka.Message {
	parts := strings.Split(fileName, "_")

	partition, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("Cannot parse partition: " + err.Error())
	}

	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		panic("Cannot parse offset: " + err.Error())
	}

	return kafka.Message{
		Topic:     s.TopicName,
		Offset:    offset,
		Partition: partition,
		Key:       []byte(fileName),
		Value:     content,
	}
}

func NewS3Source(ctx context.Context, config aws.Config, bucketName string, topicName string) *S3Source {
	s3Client := s3.NewFromConfig(config)

	return &S3Source{
		Context:    ctx,
		S3Client:   s3Client,
		BucketName: bucketName,
		TopicName:  topicName,
	}
}
