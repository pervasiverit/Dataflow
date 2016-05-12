package com.dataflow.messages;

import java.util.List;

import akka.actor.ActorRef;

public class ReadPartition extends WorkMessage{
	
	private final List<String> partitionPaths;
	
	public ReadPartition(final ActorRef workerRef, 
			final List<String> paths) {
		super(workerRef);
		this.partitionPaths = paths;
	}

	public List<String> getPartitionPaths() {
		return partitionPaths;
	}

}
