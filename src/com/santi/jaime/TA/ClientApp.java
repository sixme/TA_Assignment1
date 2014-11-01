package com.santi.jaime.TA;

import java.util.List;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class ClientApp {

	public static void main(String[] args) throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential profile by reading from the credentials file located at (C:\\Users\\Santi\\.aws\\credentials).
		 */
		AWSCredentials credentials = null;

		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
			// credentials = new InstanceProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(e.getMessage());
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.EU_WEST_1);
		sqs.setRegion(usWest2);

		// Now we look for our queue inbox

		boolean exists = false;
		String sqsInbox = "";
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
			String [] pieces = queueUrl.split("/");
			if (pieces[pieces.length-1].equals("g3-inbox")) {
				exists = true;
				sqsInbox = queueUrl;
				System.out.println("Inbox queue found");
				break;
			}
		}
		if (!exists) {
			// It doesn't exist so we create it
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("g3-inbox");
			sqsInbox = sqs.createQueue(createQueueRequest).getQueueUrl();
			System.out.println("Queue created");
		}
		String input = "";

		// We ask for user input
		Scanner sc = new Scanner(System.in);
		System.out.println("Please enter a list of comma separated integers (eg: 1,2,3,4)");
		input = sc.nextLine();
		sc.close();
		if (!input.contains(",")) {
			throw new Exception("No data comma separated");
		}

		sqs.sendMessage(new SendMessageRequest(sqsInbox, input));

		// Now we attemp to receive the response, first of all we check if the outbox queue is already created
		exists = false;
		String sqsOutbox = "";
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
			String [] pieces = queueUrl.split("/");
			if (pieces[pieces.length-1].equals("g3-outbox")) {
				exists = true;
				sqsOutbox = queueUrl;
				System.out.println("Outbox queue found");
				break;
			}
		}
		if (!exists) {
			// It doesn't exist so we create it
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("g3-outbox");
			sqsOutbox = sqs.createQueue(createQueueRequest).getQueueUrl();

		}

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsOutbox);
		int message_number = 0;
		List<Message> messages = null;
		do {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			message_number = messages.size();
		} while (message_number == 0);

		System.out.println("    Response:          " + messages.get(0).getBody());

		// Delete the message
		System.out.println("Deleting a message.\n");
		String messageRecieptHandle = messages.get(0).getReceiptHandle();
		sqs.deleteMessage(new DeleteMessageRequest(sqsOutbox, messageRecieptHandle));

	}

}
