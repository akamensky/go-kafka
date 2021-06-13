package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"time"
)

type ConsumerOptions struct {
	Topic      string
	Brokers    []string
	BufferSize int
	GroupName  string
}

type Consumer interface {
	Receive() <-chan *Message
	Errors() <-chan error
}

func NewConsumer(opts *ConsumerOptions) (Consumer, error) {
	return newConsumer(opts)
}

type consumer struct {
	opts   *ConsumerOptions
	buffer chan *Message
	errors <-chan error
}

func newConsumer(opts *ConsumerOptions) (*consumer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_3_0_0
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.AutoCommit.Enable = true
	conf.Consumer.Offsets.AutoCommit.Interval = 100 * time.Millisecond
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky

	group, err := sarama.NewConsumerGroup(opts.Brokers, opts.GroupName, conf)
	if err != nil {
		return nil, err
	}

	c := &consumer{
		opts:   opts,
		buffer: make(chan *Message, opts.BufferSize),
		errors: group.Errors(),
	}

	err = group.Consume(context.Background(), []string{opts.Topic}, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *consumer) Receive() <-chan *Message {
	return c.buffer
}

func (c *consumer) Errors() <-chan error {
	return c.errors
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		c.buffer <- &Message{Key: message.Key, Value: message.Value}
		session.MarkMessage(message, "")
	}

	return nil
}
