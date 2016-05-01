package com.dataflow.actors;

import java.util.Optional;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.RegisterWorker;
import com.dataflow.messages.WorkComplete;
import com.dataflow.messages.WorkIsReady;
import com.dataflow.messages.WorkRequest;
import com.dataflow.messages.WorkToBeDone;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Procedure;

public class WorkerExec extends UntypedActor {

	private final ActorRef worker;
	private final ActorRef jobManager;

	public WorkerExec(final Props worker , ActorRef jobManager) {
		this.worker = getContext().watch(getContext().actorOf(worker, "exec"));
		this.jobManager = jobManager;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		unhandled(message);
	}

	private final Procedure<Object> idle = new Procedure<Object>() {
		public void apply(Object message) throws Exception {
			System.out.println(message.getClass());
			MethodUtils.invokeMethod(this, "handle", message);
		}
		
		public void handle(RegisterWorker workReady) {
			sendToMaster(new RegisterWorker(getSelf()));
		}
		
		public void handle(WorkToBeDone stage) {
			worker.tell(stage, getSelf());
			getContext().become(working);
		}

		public void handle(WorkIsReady workReady) {
			sendToMaster(new WorkRequest(getSelf()));
		}
	};
	
	private final Procedure<Object> working = new Procedure<Object>() {
		public void apply(Object message) {
			if (message instanceof WorkComplete) {
				Optional<String> path = ((WorkComplete) message).getPath();
				sendToMaster(new WorkComplete(getSelf(), path, path.orElse("")));
				getContext().become(idle);
			} else if (message instanceof WorkToBeDone) {
				System.out.println("Working...");
			} else {
				unhandled(message);
			}
		}
	};

	private void sendToMaster(Object message) {
		if(message instanceof RegisterWorker){
			jobManager.tell(message, getSelf());
		}else{
			getSender().tell(message, getSelf());
		}
	}

	{
		getContext().become(idle);
	}
}
