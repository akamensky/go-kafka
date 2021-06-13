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
}

func NewAsyncProducer(opts *ProducerOptions) (AsyncProducer, error) {
	return newAsyncProducer(opts)
}

type asyncProducer struct {
	opts     *ProducerOptions
	producer sarama.AsyncProducer
	buffer   chan *sarama.ProducerMessage
	errors   chan error
}

func newAsyncProducer(opts *ProducerOptions) (*asyncProducer, error) {
	c := sarama.NewConfig()
	c.Version = sarama.V2_3_0_0
	c.Producer.Return.Errors = true
	c.Producer.RequiredAcks = sarama.WaitForLocal
	c.Producer.Compression = sarama.CompressionSnappy

	p, err := sarama.NewAsyncProducer(opts.Brokers, c)
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

func (p *asyncProducer) loop() {
	for err := range p.producer.Errors() {
		p.errors <- err
	}
}
