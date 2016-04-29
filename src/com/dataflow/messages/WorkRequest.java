package com.dataflow.messages;

import akka.actor.ActorRef;

public final class WorkRequest extends WorkMessage{

	public WorkRequest(final ActorRef workerRef) {
		super(workerRef);
	}
	
	public ActorRef getActorRef (){
		return workerRef;
	}
}
