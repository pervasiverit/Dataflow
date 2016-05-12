package com.dataflow.workers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.MapWorkComplete;
import com.dataflow.messages.ReadPartition;
import com.dataflow.messages.ReduceWorkComplete;
import com.dataflow.messages.ReduceWorkToBeDone;
import com.dataflow.messages.WorkComplete;
import com.dataflow.messages.WorkMessage;
import com.dataflow.messages.WorkToBeDone;
import com.dataflow.scheduler.CrossProductStage;
import com.dataflow.scheduler.PointWiseStage;
import com.dataflow.scheduler.Stage;
import com.dataflow.utils.Constants;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.japi.Procedure;
import akka.util.ByteString;

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
	
	@SuppressWarnings("unused")
	Procedure<Object> busy = new Procedure<Object>() {
		@Override
		public void apply(Object msg) throws Exception {
			 MethodUtils.invokeMethod(this, Constants.HANDLER, msg);
		}
		
		public void handle(WorkToBeDone workToDo) throws IOException {
			Stage stage = workToDo.getStage();
			stage.run();
			System.out.println(stage.getPartitionFiles());
			WorkComplete complete = null;
			if(stage instanceof PointWiseStage) {
				complete = new MapWorkComplete(deamonActor, 
						stage.getPartitionFiles(), stage.getTaskId());
			}
			getSender().tell(complete, deamonActor);
			manager.tell(WorkerState.IDLE, getSelf());
			getContext().unbecome();
		}
		
		public void handle(ReduceWorkToBeDone workToDo) throws Exception {
			Stage stage = workToDo.getStage();
			stage.run();
			WorkComplete complete = null;
			if(stage instanceof CrossProductStage) {
				complete = new ReduceWorkComplete(deamonActor,
						stage.getPartitionFiles(), stage.getTaskId());
			}
			getSender().tell(complete, deamonActor);
			manager.tell(WorkerState.IDLE, getSelf());
			getContext().unbecome();
		}
		
		public void handle(ReadPartition workToDo) throws Exception {
			String partitionPath = workToDo.getPartitionPath();
			byte[] data = Files.readAllBytes(Paths.get(partitionPath));
			ByteString byteString = ByteString.fromArray(data);
			
			ObjectInputStream in = new ObjectInputStream(new 
					ByteArrayInputStream(byteString.toArray()));
		
			getSender().tell(byteString, getSelf());
			manager.tell(WorkerState.IDLE, getSelf());
			getContext().unbecome();
		}
	};
	
	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println("Printing from worker :"+ msg);
		if(msg instanceof WorkMessage) {
			getSelf().tell(msg, getSender());
			manager.tell(WorkerState.BUSY, getSelf());
			getContext().become(busy);
		}
		else {
			unhandled(msg);
		}
	}
	
}
