package com.dataflow.workers;

import com.dataflow.messages.MapWorkComplete;
import com.dataflow.messages.WorkComplete;
import com.dataflow.messages.WorkToBeDone;
import com.dataflow.scheduler.PointWiseStage;
import com.dataflow.scheduler.Stage;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.japi.Procedure;

public class WorkerActor extends UntypedActor{
	
	static public enum WorkerState{
		BUSY, IDLE;
	}
	
	private final ActorRef deamonActor;
	private final ActorRef manager;
	
	public WorkerActor(ActorRef deamonActor) {
		this.deamonActor = deamonActor;
		this.manager = getContext().parent();
	}
	
	Procedure<Object> busy = new Procedure<Object>() {
		@Override
		public void apply(Object msg) throws Exception {
			if(msg instanceof WorkToBeDone) {
				WorkToBeDone work = (WorkToBeDone) msg;
				Stage stage = work.getStage();
				stage.run();
				WorkComplete complete = null;
				if(stage instanceof PointWiseStage) {
					complete = new MapWorkComplete(deamonActor, 
							((PointWiseStage)stage).getPath(), stage.getTaskId());
				}
				getSender().tell(complete, deamonActor);
				manager.tell(WorkerState.IDLE, getSelf());
				getContext().unbecome();
			}
		}
	};
	
	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println("Printing from worker :"+ msg);
		if(msg instanceof WorkToBeDone) {
			getSelf().tell(msg, getSender());
			manager.tell(WorkerState.BUSY, getSelf());
			getContext().become(busy);
		}
		else{
			unhandled(msg);
		}
	}

}
