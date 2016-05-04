package com.dataflow.messages;

import akka.actor.ActorRef;

public final class WorkRequest extends WorkMessage{
	
	final int workerCount;
	
	public WorkRequest(final ActorRef workerRef, int count) {
		super(workerRef);
		this.workerCount = count;
	}
	
	public ActorRef getActorRef (){
		return workerRef;
	}
	
	public int getWorkerCount() {
		return workerCount;
	}
}
