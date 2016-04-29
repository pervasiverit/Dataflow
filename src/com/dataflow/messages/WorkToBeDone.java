package com.dataflow.messages;

import java.util.Optional;

import com.dataflow.scheduler.Stage;

import akka.actor.ActorRef;

public class WorkToBeDone extends WorkMessage{

	final Stage stage;
	final Optional<String> path;
	
	public WorkToBeDone(final ActorRef workerRef,
			final Stage stage, final Optional<String> path) {
		
		super(workerRef);
		this.stage = stage;
		this.path = path;
	}

}
