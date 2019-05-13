package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("开始")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":"192.168.128.46",
		"group.id":"panGroup",
		"auto.offset.reset":"earliest",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	topic := "panTopic"
	c.SubscribeTopics([]string{topic}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	fmt.Println("结束")
}
