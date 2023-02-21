package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	sigv4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

func NewV4Signer(ctx context.Context, config aws.Config) *sigv4.Signer {
	value, err := config.Credentials.Retrieve(ctx)
	if err != nil {
		panic("Could not get credentials: " + err.Error())
	}

	return sigv4.NewSigner(credentials.NewStaticCredentials(
		value.AccessKeyID,
		value.SecretAccessKey,
		value.SessionToken,
	))
}
