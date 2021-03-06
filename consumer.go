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
	opts       *ConsumerOptions
	buffer     chan *Message
	errors     <-chan error
	group      sarama.ConsumerGroup
	cancelFunc context.CancelFunc
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

	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &consumer{
		opts:       opts,
		buffer:     make(chan *Message, opts.BufferSize),
		errors:     group.Errors(),
		group:      group,
		cancelFunc: cancelFunc,
	}

	go func() {
		for {
			err = group.Consume(ctx, []string{opts.Topic}, c)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
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
	c.cancelFunc()
	err := c.group.Close()
	if err != nil {
		return err
	}
	close(c.buffer)
	return nil
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
