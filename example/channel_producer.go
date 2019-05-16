package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
)

var chaMsg = make(chan string, 1)

func main() {
	fmt.Println("开始")

	brokerList := "localhost:9092,localhost:9093,localhost:9094"
	topic := "panTopic"

	produce(brokerList, topic)

	fmt.Println("结束")
}

func produce(brokerList string, topic string) {

	chaSignal := make(chan os.Signal, 1)
	signal.Notify(chaSignal, syscall.SIGINT, syscall.SIGTERM)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerList})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		panic(err)
	}
	defer p.Close()

	// 发送消息结果
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed:%v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %s [%d] at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	// 输入数据
	run := true
	go func() {
		for run {
			input := ""
			fmt.Scan(&input)
			chaMsg <- input
		}
	}()

	// 发送消息
	for run {
		select {
		case sig := <- chaSignal:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case msg := <- chaMsg:
			fmt.Printf("Trying delivery msg: %v\n", msg)
			p.ProduceChannel() <- &kafka.Message{
				TopicPartition:kafka.TopicPartition{
					Topic:&topic,
					Partition:kafka.PartitionAny,
				},
				Value:[]byte(msg),
			}
		}
	}
}
