package com.santi.jaime.TA;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class ServerApp {

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
		Region euWest1 = Region.getRegion(Regions.EU_WEST_1);
		sqs.setRegion(euWest1);
		AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());        
		String bucketName = "success-bucket-1";
		
		// We attemp to receive the request, first of all we check if the outbox queue is already created
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
			System.out.println("Queue created, beginning work!");
		}
		while (true) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsInbox);
			int message_number = 0;
			List<Message> messages = null;
			do {
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				message_number = messages.size();
			} while (message_number == 0);

			// As soon as we get the message we delete it so that the other queue doesn't get it
			String messageRecieptHandle = messages.get(0).getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(sqsInbox, messageRecieptHandle));

			String[] numbers = messages.get(0).getBody().split(",");
			Integer[] iNumbers = new Integer[numbers.length];
			for (int i = 0; i < numbers.length; i++) {
				iNumbers[i] = Integer.parseInt(numbers[i]);
			}

			// Operations
			Integer min = iNumbers[0];
			Integer max = iNumbers[0];
			Integer sum = iNumbers[0];
			Integer prod = iNumbers[0];
			for (int i = 1; i < iNumbers.length; i++) {
				if (iNumbers[i] < min)
					min = iNumbers[i];
				if (iNumbers[i] > max)
					max = iNumbers[i];

				sum += iNumbers[i];
				prod *= iNumbers[i];
			}

			// Now we attemp to send the response, first of all we check if the outbox queue is already created
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

			// We create the message to the user and insert it on outbox queue.

			String messagebody = "Max:" + max + " Min:" + min + " Prod:" + prod + " Sum:" + sum;
			SendMessageRequest message_out = new SendMessageRequest(sqsOutbox, messagebody);
			SendMessageResult message_result = sqs.sendMessage(message_out);

			System.out.println("Creating file...");
			String logname = String.valueOf(new Date().getTime());
			File logfile = new File(logname+"_log.txt");
			PrintWriter logfilepw = new PrintWriter(logfile);

			URL whatismyip = new URL("http://169.254.169.254/latest/meta-data/hostname");
			BufferedReader in = new BufferedReader(new InputStreamReader(
			                whatismyip.openStream()));

			String ip = in.readLine();
			
			
			logfilepw.println("    Timestamp:     " + new Date());
			logfilepw.println("    IP:     " + ip);;
			logfilepw.println();
			logfilepw.println("  MessageIn");
			logfilepw.println("    MessageId:     " + messages.get(0).getMessageId());
			logfilepw.println("    Body:          " + messages.get(0).getBody());
			logfilepw.println();
			logfilepw.println("  MessageOut");
			logfilepw.println("    MessageId:     " + message_result.getMessageId());
			logfilepw.println("    Body:          " + messagebody);
			logfilepw.flush();
			logfilepw.close();
			System.out.println("File created!");

			if(!s3client.doesBucketExist(bucketName)){
				s3client.createBucket(bucketName, com.amazonaws.services.s3.model.Region.EU_Ireland);
			}
			
			s3client.putObject(bucketName, logname, logfile);
		}

	}

}
