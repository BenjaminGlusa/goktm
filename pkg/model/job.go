package model

type Job struct {
	Source struct {
		Type       string   `yaml:"type"`
		RoleArn    string   `yaml:"roleArn"`
		TopicName  string   `yaml:"topicName"`
		BucketName string   `yaml:"bucketName"` // For S3 Source
		GroupId    string   `yaml:"groupId"`    // For kafka Source
		Brokers    []string `yaml:"brokers"`
	} `yaml:"source"`
	Sink struct {
		Type       string   `yaml:"type"`
		RoleArn    string   `yaml:"roleArn"`
		TopicName  string   `yaml:"topicName"`
		BucketName string   `yaml:"bucketName"` // For S3 Sink
		Brokers    []string `yaml:"brokers"`    // For Kafka Sink
	} `yaml:"sink"`
}
