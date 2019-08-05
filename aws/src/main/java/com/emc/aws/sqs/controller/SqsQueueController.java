package com.emc.aws.sqs.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.emc.aws.sqs.model.Messages;
import com.emc.aws.sqs.model.Queues;

/**
 * 
 * @author singhm32
 *
 */


@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
@RequestMapping("/sqs")
public class SqsQueueController {
	
	final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

	@GetMapping(value = "/create", produces = "application/json")
	public @ResponseBody String createQueue(@RequestParam String queueName) {
		
		int status = 0;
		String myQueueUrl = "";
		
		try {

			// Create a queue
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			status = 1;

		} catch (final AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was "
							+ "rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			
			status =0;
		} catch (final AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
			
			status =0;
		}
		 
		if(status ==1)
			return "Successfully created : "+ myQueueUrl;
		else
			return "Failed to create the given queue";
	
	}
	
	@GetMapping(value = "/list", produces = "application/json")
	public @ResponseBody Queues listQueues() {
		
		Queues queueList  = new Queues();
		
		try {
			List<String> queues = new ArrayList<String>();

			for (final String queueUrl : sqs.listQueues().getQueueUrls()) {
			queues.add(queueUrl);
			}

			queueList.setQueueNames(queues);
			
		} catch (final AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was "
							+ "rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());

		} catch (final AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());

		}
		
		return queueList;
	}
	
	@PostMapping(value = "/sendMessage", consumes = "application/json" ,produces = "application/json")
	public @ResponseBody String sendMessage(@RequestParam String queueName, @RequestBody Messages message) {
		
		int status = 0;
		
		try {
			 GetQueueUrlResult queueUrl = sqs.getQueueUrl(queueName);
				
			 sqs.sendMessage(new SendMessageRequest(queueUrl.getQueueUrl(), message.getMessage()));
			 status = 1;
			}
		catch (final AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was "
							+ "rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			
			status =0;
		} catch (final AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
			
			status =0;
		}
		
		if(status ==1)
			return "Message Sent !";
		else
			return "Message failed to send !";
		
		
	}
	
	@GetMapping(value = "/getMessages", produces = "application/json")
	public @ResponseBody List<Message> getMessages(@RequestParam String queueName) {
		
		//List<Messages> messageList = null;
		ReceiveMessageRequest receiveMessageRequest = null;
		List<Message> messages = null;
		
		try {
			GetQueueUrlResult queueUrl = sqs.getQueueUrl(queueName);
			
			receiveMessageRequest =
                    new ReceiveMessageRequest(queueUrl.getQueueUrl());
            messages = sqs.receiveMessage(receiveMessageRequest)
                    .getMessages();
            
           // messageList.add((Messages) messages);
			
			
		} catch (final AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was "
							+ "rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());

		} catch (final AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());

		}
		
		return messages; 
		
	}
	
	@DeleteMapping(value = "/deleteQueue", produces = "application/json")
	public @ResponseBody String deleteQueue(@RequestParam String queueName) { 
		
		int status = 0;
		
		try {
			GetQueueUrlResult queueUrl = sqs.getQueueUrl(queueName);
			
			sqs.deleteQueue(new DeleteQueueRequest(queueUrl.getQueueUrl()));
			 
			status = 1;
			
			
		} catch (final AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was "
							+ "rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			
			status = 0;

		} catch (final AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
			
			status = 0;

		}
		
		if(status ==1)
			return "Queue deleted successfully !";
		else
			return "Error while deleting queue !";
	}

}
