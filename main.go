package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func main()  {
	// Example usage

	// create cluster admin
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.MaxVersion
	admin, err := sarama.NewClusterAdmin([]string{"broker:9092"}, kafkaConfig)
	if err != nil {
		log.Fatalf("Error creating admin: %v",err.Error())
	}
	defer func() { _ = admin.Close() }()

	// create a topic
	err = admin.CreateTopic("my_topic", &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	if err != nil {
		log.Printf("Create topic error: %v", err)
	}
	// clone it
	err = CloneTopic(admin, "my_topic", "my_topic-replica")
	if err != nil {
		log.Printf("Clone topic error: %v", err)
	}

	// view all topics. Ge their topic names to verify creation

	var topicNames []string
	topicNames, err = getClusterTopicNames(admin)
	if err != nil{
		log.Printf("get topic names error: %v", err)
	}

	fmt.Printf("Topic Names: %v ", topicNames)

}
