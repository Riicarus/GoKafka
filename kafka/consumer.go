package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

var (
	// each client uses a auto-increament consumer id from 0
	ConsumerId = 0
)


type Consumer struct {
	Consumer   sarama.Consumer
	Topic      string
	ConsumerId int
}

func (c *Consumer) Init(topic string) error {
	consumer, err := sarama.NewConsumer(ServerList(), nil)
	if err != nil {
		return err
	}

	c.Consumer = consumer
	c.Topic = topic
	c.ConsumerId = ConsumerId
	ConsumerId++

	return nil
}

func (c *Consumer) GetMessage(partition int32, offset int64) {
	// auto choose to newest offset when partition id is -1
	if offset == -1 {
		offset = sarama.OffsetNewest
	}

	pc, err := c.Consumer.ConsumePartition(c.Topic, partition, offset)
	if err != nil {
		fmt.Printf("fail to start consumer for partition %d, err: %v\n", partition, err)
		return
	}

	// create a routine to consume message asynchronously
	go func (sarama.PartitionConsumer)  {
		for msg := range pc.Messages() {
			fmt.Printf("ConsumerId: %d, partition: %d, offset: %d, key: %d value: %v\n", c.ConsumerId, msg.Partition, msg.Offset, msg.Key, string(msg.Value))
		}
	}(pc)
}

func (c *Consumer) GetMessageFromAll(offset int64) {
	// get all partitions of the given topic
	partitionList, err := c.Consumer.Partitions(c.Topic)

	if err != nil {
		fmt.Printf("fail to get partition list of topic %s, err: %v\n", c.Topic, err)
		return
	}

	fmt.Println("all partitions: ", partitionList)

	for partition := range partitionList {
		c.GetMessage(int32(partition), offset)
	}
}