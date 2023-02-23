# Go Kafka Topic Mover

The Go Kafka Topic Mover aims to move all messages of an Apache Kafka Topic to another one as backup or cluster migration.

At the moment the target is an aws msk cluster. So goktm assumes that there is an iam role to use for authentication.

Furthermore, the messages are stored in a s3 bucket before they are re-published to the new topic/cluster. 

**IMPORTANT**

The project is in an early proof of concept state and **should not be used in production**.

## Prerequisites

You need an iam role for goktm to assume which has access to the kafka topic you want to move and a s3 bucket to store messages in.
Here's a sample policy for such a role:

```yaml
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:ReadData",
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
            ],
            "Resource": [
                "arn:aws:kafka:eu-west-1:<account-id>:cluster/<cluster-id>/*",
                "arn:aws:kafka:eu-west-1:<account-id>:group/<cluster-id>/*",
                "arn:aws:kafka:eu-west-1:<account-id>:topic/<cluster-id>/*/<topic-name>"
            ],
            "Effect": "Allow"
        },
        {
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket-name>/*",
                "arn:aws:s3:::<bucket-name>"
            ],
            "Effect": "Allow"
        }
    ]
}

```

## Usage

You need to build the project first by running:

```sh
make build
```

Please also set an environment variable for your aws region:

```sh
export AWS_REGION=eu-west-1
```

Create a yaml file such as `job.yml` to configure your job. See [doc folder](doc) for examples.

Now you can run goktm with the following command:

```sh
/bin/goktm-linux-386 \
  --options-yaml job.yml
```
