package com.dataflow.messages;

import akka.actor.ActorRef;

public class NodeTerminated extends WorkMessage{

	public NodeTerminated(ActorRef workerRef) {
		super(workerRef);
	}

}
