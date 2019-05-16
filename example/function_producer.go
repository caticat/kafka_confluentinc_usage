/**
 * 这里有一个问题
 * kafka.Producer.Produce函数是非阻塞调用
 * 调用的结果通过参数的chan返回
 * 当libkafka库内部的日志缓冲满了的时候回返回错误
 * 需要注意捕捉并处理
 * 就是日志太多了会发送失败,不处理会丢数据
 */

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
	defer close(chaSignal)
	signal.Notify(chaSignal, syscall.SIGINT, syscall.SIGTERM)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerList})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		panic(err)
	}
	defer p.Close()

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
	chaResult := make(chan kafka.Event, 5)
	defer close(chaResult)
	for run {
		select {
		case sig := <- chaSignal:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case msg := <- chaMsg:
			fmt.Printf("Trying delivery msg: %v\n", msg)
			p.Produce(&kafka.Message{
				TopicPartition:kafka.TopicPartition{
					Topic:&topic,
					Partition:kafka.PartitionAny,
				},
				Value:[]byte(msg),
				Headers:[]kafka.Header{
					{
						Key:"myTestHeader",
						Value:[]byte("header values are binary"),
					},
				},
			}, chaResult)
		case e := <- chaResult:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivery message to topic %s [%d] att offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}
	}
}
