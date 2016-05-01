package com.dataflow.actors;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.WorkRequest;
import com.dataflow.scheduler.PointWiseStage;

import akka.actor.UntypedActor;

public class WorkerActor extends UntypedActor{
	private boolean isRunning;
	private ScheduledFuture scheduledFuture;
	
	public WorkerActor(){
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MethodUtils.invokeMethod(this, "handler",message);
	}
	
	public void handler(WorkRequest request){
		getSender().tell(request, getSelf());
	}
	
	public void handler(PointWiseStage stg){
		try {
			stg.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
