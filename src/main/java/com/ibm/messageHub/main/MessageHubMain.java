package com.ibm.messageHub.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.http.client.ClientProtocolException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.messageHub.MessageConsumer;
import com.ibm.messageHub.MessageConsumerWithCustomization;
import com.ibm.messageHub.MessageHubTopicService;
import com.ibm.messageHub.MessageProducer;
import com.ibm.messageHub.beans.TopicConfig;
import com.ibm.messageHub.beans.TopicParameters;

public class MessageHubMain {

	public static void main(String[] args) throws ClientProtocolException, IOException, URISyntaxException, InterruptedException {

		Properties messageHubCreds = getMessageHubCredentials();
		String apiKey = messageHubCreds.getProperty("apiKey");
		String kafkaAdminUrl = messageHubCreds.getProperty("kafkaAdminUrl");
		String kafkaBrokers = messageHubCreds.getProperty("kafkaBrokers");
		TopicParameters topicParams = new TopicParameters("SampleTopic", 1, new TopicConfig(3600000L * 24));

		MessageHubTopicService topicService = new MessageHubTopicService(kafkaAdminUrl, apiKey);
		topicService.deleteTopic(topicParams.getName());
		topicService.createTopic(topicParams);
		topicService.listTopics();

		setJaasConfiguration(apiKey.substring(0, 16), apiKey.substring(16));
		Properties producerProps = getProperties("producer.properties", kafkaBrokers);
		Properties consumerProps = getProperties("consumer.properties", kafkaBrokers);
		
		MessageProducer producer = new MessageProducer(producerProps, topicParams.getName());
		Thread producerThread = new Thread(producer);
		producerThread.start();
		
		MessageConsumer consumer = new MessageConsumer(consumerProps, topicParams.getName());
//		MessageConsumerWithCustomization consumer = new MessageConsumerWithCustomization(consumerProps, topicParams.getName());
		Thread consumerThread = new Thread(consumer);
		consumerThread.start();
	}

	private static Properties getProperties(String propertiesFile, String bootstrapServers) throws IOException {
		Properties props = new Properties();
		props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFile));
		props.put("bootstrap.servers", bootstrapServers);

		return props;
	}

	private static Properties getMessageHubCredentials() {
		Properties messageHubCreds = new Properties();

		InputStream inputStream = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("messageHubCredentials.json");
		JsonElement jsonElement = new JsonParser().parse(new InputStreamReader(inputStream));
		JsonObject jsonObject = jsonElement.getAsJsonObject();

		messageHubCreds.put("apiKey", jsonObject.get("api_key").getAsString());
		messageHubCreds.put("kafkaAdminUrl", jsonObject.get("kafka_admin_url").getAsString());
		JsonArray kafkaBrokers = jsonObject.get("kafka_brokers_sasl").getAsJsonArray();
		StringBuilder bootstrapServers = new StringBuilder();
		for (JsonElement broker : kafkaBrokers) {
			bootstrapServers.append(broker.getAsString());
			bootstrapServers.append(",");
		}
		bootstrapServers.deleteCharAt(bootstrapServers.length() - 1);
		messageHubCreds.put("kafkaBrokers", bootstrapServers.toString());

		return messageHubCreds;
	}

	private static void setJaasConfiguration(String username, String password) throws IOException, URISyntaxException {
		// Set JAAS configuration property.
		String jaasConfPath = System.getProperty("java.io.tmpdir") + File.separator + "jaas.conf";
		System.setProperty("java.security.auth.login.config", jaasConfPath);

		URL resource = Thread.currentThread().getContextClassLoader().getResource("jaas.conf.template");
		OutputStream jaasOutStream = null;

		System.out.println("Updating JAAS configuration");

		try {
			String templateContents = new String(Files.readAllBytes(Paths.get(resource.toURI())));
			jaasOutStream = new FileOutputStream(jaasConfPath, false);

			// Replace username and password in template and write
			// to jaas.conf in resources directory.
			String fileContents = templateContents.replace("$USERNAME", username).replace("$PASSWORD", password);

			jaasOutStream.write(fileContents.getBytes(Charset.forName("UTF-8")));
		} catch (final IOException e) {
			throw e;
		} finally {
			if (jaasOutStream != null) {
				try {
					jaasOutStream.close();
				} catch (final Exception e) {
				}
			}
		}
	}
}
