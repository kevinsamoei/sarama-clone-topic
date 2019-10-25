package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func CloneTopic(admin sarama.ClusterAdmin, sourceTopicName string, destinationTopicName string) error {
	settings := getTopicSettings(admin, sourceTopicName)

	configEntries := map[string]*string{}
	for _, value := range settings{
		v := value.Value
		if !value.Sensitive || !value.ReadOnly || !value.Default {
			configEntries[value.Name] = &v
		}
	}
	_, partitionMetadata := getTopicDetails(sourceTopicName, admin)
	numOfPartitions := int32(len(partitionMetadata))
	details :=  &sarama.TopicDetail{
		NumPartitions:     numOfPartitions,
		ReplicationFactor: 1,
		ConfigEntries:     configEntries,
	}
	err := admin.CreateTopic(destinationTopicName, details, false)
	if err != nil {
		log.Fatalf("create topic error: %v", err)
		return err
	}
	return nil

}

func getClusterTopicNames(admin sarama.ClusterAdmin) ([]string, error){
	topics := map[string]sarama.TopicDetail{}
	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("List topics error: %v", err)
	}
	var topicName []string
	for t := range topics {
		topicName = append(topicName, t)
	}
	return topicName, nil
}

func getTopicSettings(admin sarama.ClusterAdmin, topicName string) []sarama.ConfigEntry{
	var entry []sarama.ConfigEntry
	topicResource := sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        topicName,
	}
	entry, err := admin.DescribeConfig(topicResource)
	if err != nil {
		fmt.Printf("Describe topic Config error: %v",err)
	}
	log.Printf("Topic configs: %v\n", entry)
	return entry
}

// TODO: confirm this works
func cloneTopicPartitions(admin sarama.ClusterAdmin, topic string) error {
	_, partitionMetadata := getTopicDetails(topic, admin)
	var assignment [][]int32
	for _, v := range partitionMetadata{
		assignment = append(assignment, []int32{v.ID}, v.Isr, v.Replicas, v.OfflineReplicas)
	}
	numOfPartitions := int32(len(partitionMetadata))
	err := admin.CreatePartitions(topic, numOfPartitions, assignment, false)
	if err != nil{
		return err
	}
	return nil
}

// Important for getting existing topic partitions
func getTopicPartitionDetails(topic string, admin sarama.ClusterAdmin) map[string]interface{} {
	_, partitionMetadata := getTopicDetails(topic, admin)
	partitionDetailsMap := make(map[string]interface{})
	for _, p := range partitionMetadata{
		partitionDetailsMap["partition_id"] = p.ID
		partitionDetailsMap["partition_isr"] = p.Isr
		partitionDetailsMap["partition_leader"] = p.Leader
		partitionDetailsMap["partition_offline_replicas"] = p.OfflineReplicas
		partitionDetailsMap["partition_replicas"] = p.Replicas
	}
	return partitionDetailsMap
}

func getTopicDetails(topic string, admin sarama.ClusterAdmin) (string, []*sarama.PartitionMetadata) {
	metadata, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		fmt.Printf("Describe topic Error: %v",err)
	}
	topicName := ""
	var partitionMetadata []*sarama.PartitionMetadata
	for _, v := range metadata{
		topicName = v.Name
		partitionMetadata = v.Partitions
	}
	return topicName, partitionMetadata
}