package kafka

import (
	"context"
	"github.com/Shopify/sarama"
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
	Close() error
}

func NewConsumer(opts *ConsumerOptions, config *sarama.Config) (Consumer, error) {
	return newConsumer(opts, config)
}

type consumer struct {
	opts   *ConsumerOptions
	buffer chan *Message
	errors <-chan error
	group  sarama.ConsumerGroup
}

func newConsumer(opts *ConsumerOptions, config *sarama.Config) (*consumer, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	group, err := sarama.NewConsumerGroup(opts.Brokers, opts.GroupName, config)
	if err != nil {
		return nil, err
	}

	c := &consumer{
		opts:   opts,
		buffer: make(chan *Message, opts.BufferSize),
		errors: group.Errors(),
		group:  group,
	}

	go func() {
		ctx := context.Background()
		for {
			err = group.Consume(ctx, []string{opts.Topic}, c)
			if err != nil {
				panic(err)
			}
		}
	}()

	return c, nil
}

func (c *consumer) Receive() <-chan *Message {
	return c.buffer
}

func (c *consumer) Errors() <-chan error {
	return c.errors
}

func (c *consumer) Close() error {
	return c.group.Close()
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
