package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

var (
	// each client uses a auto-increament producer id from 0
	ProducerId = 0
)

type SyncProducer struct {
	Producer   sarama.SyncProducer
	Topic      string
	ProducerId int
	MessageId  int
}

// create a sync producer, need to close manually
func (p *SyncProducer) Init(topic string) error {
	config := sarama.NewConfig()
	// consistency: ensure each messsage can be load by all replicas
	config.Producer.RequiredAcks = sarama.WaitForAll
	// randomly choose a partition to send message to
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// consistency: return success message to success channel
	config.Producer.Return.Successes = true

	// connect to kafka
	client, err := sarama.NewSyncProducer(ServerList(), config)
	if err != nil {
		return err
	}

	p.Producer = client
	p.Topic = topic
	p.ProducerId = ProducerId
	p.MessageId = 0

	ProducerId++

	return nil
}

// sync-send message
func (p *SyncProducer) SendMessage(value string) {
	// create a message
	msg := &sarama.ProducerMessage{}
	msg.Topic = p.Topic
	payload := fmt.Sprintf("%d&&%d&&%s", p.ProducerId, p.MessageId, value)
	msg.Value = sarama.StringEncoder(payload)

	// send message
	partition, offset, err := p.Producer.SendMessage(msg)
	if err != nil {
		fmt.Println("kafka send message err: ", err)
		return
	}

	fmt.Printf("ProducerId: %d, partition: %d, offset: %d, msg: %s\n", p.ProducerId, partition, offset, payload)

	p.MessageId++
}

func (p *SyncProducer) Close() {
	p.Producer.Close()
}
