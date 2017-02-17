package com.ibm.messageHub;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.ibm.messageHub.beans.TopicParameters;

public class MessageHubTopicService {

	private String topicUrl;
	private String messageHubApiKey;

	public MessageHubTopicService(String kafkaAdminUrl, String messageHubApiKey) {
		super();
		this.topicUrl = kafkaAdminUrl + "/admin/topics";
		this.messageHubApiKey = messageHubApiKey;
	}

	public void createTopic(TopicParameters topicParams) throws ClientProtocolException, IOException {
		System.out.println("Creating the topic " + topicParams.getName());

		Gson gson = new Gson();
		StringEntity entity = new StringEntity(gson.toJson(topicParams));
		HttpPost postRequest = new HttpPost(topicUrl);
		postRequest.setEntity(entity);

		HttpResponse response = sendRequest(postRequest);

		System.out.println("Topic created. " + response.getStatusLine());
	}

	public void deleteTopic(String topicName) throws ClientProtocolException, IOException {
		System.out.println("Deleting the topic " + topicName);

		String url = topicUrl + "/" + topicName;
		HttpDelete deleteRequest = new HttpDelete(url);

		HttpResponse response = sendRequest(deleteRequest);

		System.out.println("Topic deleted. " + response.getStatusLine());
	}

	public void listTopics() throws ClientProtocolException, IOException {
		System.out.println("Listing topics");

		HttpGet getRequest = new HttpGet(topicUrl);

		HttpResponse response = sendRequest(getRequest);

		InputStreamReader ir = new InputStreamReader(response.getEntity().getContent());
		JsonElement je = new JsonParser().parse(ir);
		System.out.println("Topics: " + je.toString());
	}

	private HttpResponse sendRequest(HttpRequestBase request) throws IOException, ClientProtocolException {
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();

		request.addHeader("X-Auth-Token", messageHubApiKey);
		request.addHeader("Content-Type", "application/json");

		HttpResponse response = httpClient.execute(request);
		return response;
	}
}
