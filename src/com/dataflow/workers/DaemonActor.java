package com.dataflow.workers;

import com.dataflow.messages.ConnectionComplete;
import com.dataflow.messages.WorkIsReady;
import com.dataflow.messages.WorkToBeDone;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class DaemonActor extends UntypedActor{
	
	private ActorRef manager;

	@Override
	public void preStart() throws Exception {
		manager = getContext().actorOf(Props.
				create(WorkerManager.class), "WorkerManager");
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof WorkToBeDone || msg instanceof WorkIsReady)
			manager.forward(msg, getContext());
		else if(msg instanceof ConnectionComplete)
			manager.forward(msg, getContext());
		else 
			unhandled(msg);
	}

}
