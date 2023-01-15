package main

import (
	"fmt"
	"time"

	"github.com/riicarus/go-kafka/kafka"
)

func main() {
	go Put()
	go Get()

	for {
		time.Sleep(time.Hour * 24)
	}
}

func Put() {
	syncProducer := &kafka.SyncProducer{}
	err := syncProducer.Init(kafka.WEB_LOG_TOPIC)
	if err != nil {
		fmt.Println("connect to kafka err: ", err)
	}

	i := 0
	for {
		syncProducer.SendMessage(fmt.Sprintf("this is a web log[%d]", i))
		i++

		time.Sleep(1 * time.Second)
	}
}

func Get() {
	offset := int64(0)

	consumer := new(kafka.Consumer)
	err := consumer.Init(kafka.WEB_LOG_TOPIC)
	if err != nil {
		fmt.Println("fail to init kafka consumer, err: ", err)
		return
	}

	consumer.GetMessageFromAll(offset)
}