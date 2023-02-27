package model

import (
	"fmt"
	"strconv"
	"strings"
	"github.com/segmentio/kafka-go"
)


type KafkaClient interface {
	
}

func KafkaMessageFromMessage(message Message) kafka.Message {
	topic, partition, offset := SplitKafkaId(message.Id)

	return kafka.Message{
		Topic: topic,
		Partition: partition,
		Offset: offset,
		Value: []byte(message.Text),
	}
}


func MessageFromKafkaMessage(message kafka.Message) Message {

	return Message{
		Id: fmt.Sprintf("%s_%d_%d", message.Topic, message.Partition, message.Offset),
		Text: string(message.Value),
	}
}


func SplitKafkaId(kafkaId string) (string, int, int64) {
	parts := strings.Split(kafkaId, "_")
	
	topic := parts[0]
	partition, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("Cannot parse partition: " + err.Error())
	}
	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		panic("Cannot parse offset: " + err.Error())
	}

	return topic, partition, offset
}