package com.emc.aws.sqs.model;

import java.util.List;

public class Queues {

	private List<String> queueNames;
	
	public Queues() {
		super();
	}

	public Queues(List<String> queueNames) {
		super();
		this.queueNames = queueNames;
	}

	public List<String> getQueueNames() {
		return queueNames;
	}

	public void setQueueNames(List<String> queueNames) {
		this.queueNames = queueNames;
	}
	
}
