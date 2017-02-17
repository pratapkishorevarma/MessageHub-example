package com.ibm.messageHub;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;

public class MessageProducer implements Runnable {

	private final KafkaProducer<String, String> kafkaProducer;
	private final String topicName;

	public MessageProducer(Properties producerProperties, String topicName) {
		this.topicName = topicName;
		this.kafkaProducer = new KafkaProducer<String, String>(producerProperties);

		try {
			// Checking for topic existence.
			// If the topic does not exist, the kafkaProducer will retry for
			// about 60 secs before throwing a TimeoutException
			List<PartitionInfo> partitions = kafkaProducer.partitionsFor(topicName);
			System.out.println(partitions.toString());
		} catch (TimeoutException e) {
			System.out.println("Topic '" + topicName + "' does not exist");
			kafkaProducer.close();
			throw new IllegalStateException("Topic '" + topicName + "' does not exist", e);
		}
	}

	public void run() {
		int producedMessages = 0;
		System.out.println(MessageProducer.class.getName() + " is starting.");

		try {
			while (true) {

				String key = "key";
				String message = "This is a test message #" + producedMessages;

				// If a partition is not specified, the client will use the
				// default partitioner to choose one.
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, message);

				// Send record asynchronously
				Future<RecordMetadata> future = kafkaProducer.send(record);
				// Synchronously wait for a response from Message Hub /
				// Kafka on every message produced.
				// For high throughput the future should be handled
				// asynchronously.
				RecordMetadata recordMetadata = future.get(5000, TimeUnit.MILLISECONDS);
				producedMessages++;

				System.out.println("Message produced, offset: " + recordMetadata.offset());

				Thread.sleep(2000);
			}
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			kafkaProducer.close();
			System.out.println(MessageProducer.class.toString() + " has shut down.");
		}
	}
}
