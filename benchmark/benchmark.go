package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/akamensky/argparse"
	"github.com/akamensky/go-kafka"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	p := argparse.NewParser("benchmark", "Tool to benchmark this library")

	brokersArg := p.String("b", "brokers", &argparse.Options{
		Required: false,
		Help:     "Comma separated list of brokers",
		Default:  "localhost:9092",
	})

	topicArg := p.String("t", "topic-name", &argparse.Options{
		Required: false,
		Help:     "Name of topic to use for benchmarking",
		Default:  "benchmark",
	})

	err := p.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	brokers := strings.Split(*brokersArg, ",")
	topic := *topicArg

	go produceTracking()
	go produce(brokers, topic)

	go consumeTracking()
	go consume(brokers, topic)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

var produceCounter uint64 = 0

func produce(brokers []string, topic string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_3_0_0
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal

	producer, err := kafka.NewAsyncProducer(
		&kafka.ProducerOptions{Topic: topic, Brokers: brokers},
		config,
	)
	if err != nil {
		panic(err)
	}
	go func() {
		for err := range producer.Errors() {
			if err != nil {
				panic(err)
			}
		}
	}()
	for {
		producer.Send(&kafka.Message{
			Key:   nil,
			Value: []byte("test"),
		})
		produceCounter++
	}
}

func produceTracking() {
	for {
		time.Sleep(1 * time.Second)
		fmt.Printf("Produced: %d\n", produceCounter)
		produceCounter = 0
	}
}

var consumeCounter uint64 = 0

func consume(brokers []string, topic string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_3_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := kafka.NewConsumer(
		&kafka.ConsumerOptions{Topic: topic, Brokers: brokers, GroupName: "benchmark"},
		config,
	)
	if err != nil {
		panic(err)
	}
	go func() {
		for err := range consumer.Errors() {
			if err != nil {
				panic(err)
			}
		}
	}()
	for {
		for range consumer.Receive() {
			consumeCounter++
		}
	}
}

func consumeTracking() {
	for {
		time.Sleep(1 * time.Second)
		fmt.Printf("Consumed: %d\n", consumeCounter)
		consumeCounter = 0
	}
}
