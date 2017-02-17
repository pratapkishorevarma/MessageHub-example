package com.ibm.messageHub;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Consumer class which uses various options to have more control on consuming
 * messages
 * 
 * @author pratap
 *
 */
public class MessageConsumerWithCustomization implements Runnable {

	private final KafkaConsumer<String, String> kafkaConsumer;
	private final TopicPartition topicPartition;
	private long offset = -1;

	public MessageConsumerWithCustomization(Properties consumerProperties, String topicName) {

		// Making enable.auto.commit false doesn't commit the completed offset
		// to kafka. Need this setting when we want to set offset manually.
		consumerProperties.put("enable.auto.commit", "false");
		consumerProperties.put("max.poll.records", 10);
		consumerProperties.put("auto.offset.reset", "none");

		this.kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);

		// Checking for topic existence before subscribing
		List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topicName);
		if (partitions == null || partitions.isEmpty()) {
			System.out.println("Topic '" + topicName + "' does not exist");
			kafkaConsumer.close();
			throw new IllegalStateException("Topic '" + topicName + "' does not exist");
		} else {
			System.out.println(partitions.toString());
		}

		// subscribe to a partition in topic
		this.topicPartition = new TopicPartition(topicName, 0);
		this.kafkaConsumer.assign(Collections.singletonList(topicPartition));
	}

	public void run() {
		System.out.println(MessageConsumerWithCustomization.class.getName() + " is starting.");

		try {
			while (true) {
				// Setting the offset manually
				if (offset == -1) {
					// Setting consumer to seekToBeginning will get all the
					// messages in the partition, which are published before
					// starting the consumer
					kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
				} else {
					kafkaConsumer.seek(topicPartition, offset);
				}

				// Poll on the Kafka consumer, waiting up to 3 secs if
				// there's nothing to consume.
				ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);

				if (records.isEmpty()) {
					System.out.println("No messages consumed");
				} else {
					// Iterate through all the messages received and print
					// their content
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("Message consumed: " + record.toString());
						// Setting the offset will give more control on handling
						// message ie., offset can be set only after the message
						// is processed successfully.
						offset = record.offset() + 1;
					}
				}
			}
		} finally {
			kafkaConsumer.close();
			System.out.println(MessageConsumerWithCustomization.class.getName() + " has shut down.");
		}
	}
}
