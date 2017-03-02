package com.ibm.messageHub;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

public class MessageConsumer implements Runnable {

	private final KafkaConsumer<String, String> kafkaConsumer;

	public MessageConsumer(Properties consumerProperties, String topicName) {

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

		this.kafkaConsumer.subscribe(Arrays.asList(topicName));
	}

	public void run() {
		System.out.println(MessageConsumer.class.getName() + " is starting.");

		try {
			while (true) {
				// Poll on the Kafka consumer, waiting up to 3 secs if
				// there's nothing to consume.
				ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);

				if (records.isEmpty()) {
					System.out.println("No messages consumed by 1");
				} else {
					// Iterate through all the messages received and print
					// their content
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("Message consumed by 1: " + record.toString());
					}
				}
			}
		} finally {
			kafkaConsumer.close();
			System.out.println(MessageConsumer.class.getName() + " has shut down.");
		}
	}
}
