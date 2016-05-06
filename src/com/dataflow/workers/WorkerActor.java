package com.dataflow.workers;

import akka.actor.UntypedActor;

public class WorkerActor extends UntypedActor{
	
	static public enum WorkerState{
		BUSY, IDLE;
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println("Printing from worker :"+ msg);
		if(msg instanceof String) {
			getContext().parent().tell(WorkerState.BUSY, getSelf());
			Thread.sleep(4000);
			getSender().tell("Hello Manager", getSelf());
			getContext().parent().tell(WorkerState.IDLE, getSelf());
		}
		else{
			unhandled(msg);
		}
	}

}
