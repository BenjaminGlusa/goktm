package aws

import (
	"context"
	"fmt"
	"github.com/BenjaminGlusa/goktm/pkg/random"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"os"
)

func AssumeRole(ctx context.Context, roleArn string) aws.Config {
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
		fmt.Printf("Couldn't assume role %v.\n", roleArn)
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

	return assumeRoleConfig
}
