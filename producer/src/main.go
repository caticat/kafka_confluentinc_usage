package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("开始")
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// 发送消息结果
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed:%v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// 发送消息
	topic := "panTopic"
	for _, msg := range []string{"aaa", "bbb", "ccc"} {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil)
		fmt.Printf("发送:%v,错误:%v\n", msg, err)
	}

	p.Flush(15 * 1000)
	fmt.Println("结束")
}
