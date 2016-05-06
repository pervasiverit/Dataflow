package com.dataflow.workers;

import com.dataflow.messages.WorkToBeDone;

import akka.actor.UntypedActor;
import akka.japi.Procedure;

public class WorkerActor extends UntypedActor{
	
	static public enum WorkerState{
		BUSY, IDLE;
	}
	
	Procedure<Object> busy = new Procedure<Object>() {
		@Override
		public void apply(Object msg) throws Exception {
			if(msg instanceof WorkToBeDone) {
				//TODO : Execute the work here
				getContext().parent().tell(WorkerState.IDLE, getSelf());
				getContext().unbecome();
			}
		}
	};
	
	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println("Printing from worker :"+ msg);
		if(msg instanceof WorkToBeDone) {
			getSelf().tell(msg, getSender());
			getContext().parent().tell(WorkerState.BUSY, getSelf());
			getContext().become(busy);
		}
		else{
			unhandled(msg);
		}
	}

}
