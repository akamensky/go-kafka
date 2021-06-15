package kafka

import (
	"github.com/Shopify/sarama"
)

type ProducerOptions struct {
	Topic      string
	Brokers    []string
	BufferSize int
}

type AsyncProducer interface {
	Send(message *Message)
	Errors() <-chan error
	Close() error
}

func NewAsyncProducer(opts *ProducerOptions, config *sarama.Config) (AsyncProducer, error) {
	return newAsyncProducer(opts, config)
}

type asyncProducer struct {
	opts     *ProducerOptions
	producer sarama.AsyncProducer
	buffer   chan *sarama.ProducerMessage
	errors   chan error
}

func newAsyncProducer(opts *ProducerOptions, config *sarama.Config) (*asyncProducer, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewAsyncProducer(opts.Brokers, config)
	if err != nil {
		return nil, err
	}

	bufferSize := 4096
	if opts.BufferSize > 0 {
		bufferSize = opts.BufferSize
	}

	producer := &asyncProducer{
		opts:     opts,
		buffer:   make(chan *sarama.ProducerMessage, bufferSize),
		producer: p,
	}

	go producer.loop()

	return producer, nil
}

func (p *asyncProducer) Send(message *Message) {
	p.producer.Input() <- &sarama.ProducerMessage{Topic: p.opts.Topic, Key: sarama.ByteEncoder(message.Key), Value: sarama.ByteEncoder(message.Value)}
}

func (p *asyncProducer) Errors() <-chan error {
	return p.errors
}

func (p *asyncProducer) Close() error {
	return p.producer.Close()
}

func (p *asyncProducer) loop() {
	for err := range p.producer.Errors() {
		p.errors <- err
	}
}
