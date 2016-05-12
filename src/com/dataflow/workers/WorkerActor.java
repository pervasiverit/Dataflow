package com.dataflow.workers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.elements.Element;
import com.dataflow.messages.MapWorkComplete;
import com.dataflow.messages.ReadPartition;
import com.dataflow.messages.ReduceWorkToBeDone;
import com.dataflow.messages.WorkComplete;
import com.dataflow.messages.WorkMessage;
import com.dataflow.messages.WorkToBeDone;
import com.dataflow.scheduler.PointWiseStage;
import com.dataflow.scheduler.Stage;
import com.dataflow.utils.Constants;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.japi.Procedure;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class WorkerActor extends UntypedActor{
	
	static public enum WorkerState{
		BUSY, IDLE;
	}
	
	private final ActorRef deamonActor;
	private final ActorRef manager;
	private final ActorSystem system = getContext().system();
	
	public WorkerActor(ActorRef deamonActor) {
		this.deamonActor = deamonActor;
		this.manager = getContext().parent();
	}
	
	Procedure<Object> busy = new Procedure<Object>() {
		@Override
		public void apply(Object msg) throws Exception {
			 MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
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
		else{
			unhandled(msg);
		}
	}
	
	public void handle(WorkToBeDone workToDo) throws IOException {
		Stage stage = workToDo.getStage();
		stage.setPartitionCount(2);
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
		Map<ActorRef, List<String>> mappers = workToDo.getActorPathMapping();
		List<Future<Object>> futures = new ArrayList<>();
		Timeout timeout = new Timeout(Duration.create(10, "seconds"));
		mappers.forEach((mapper, partitionPaths)->futures.add(Patterns
				.ask(mapper, new ReadPartition(getSelf(), partitionPaths),
						timeout)));
		Await.result(Futures.sequence(futures, system.dispatcher()), 
				timeout.duration());
		
		
		WorkComplete complete = null;
		getSender().tell(complete, deamonActor);
		manager.tell(WorkerState.IDLE, getSelf());
		getContext().unbecome();
	}
	
	public void handle(ReadPartition workToDo) throws Exception {
		List<String> partitionPaths = workToDo.getPartitionPaths();
		for(String path : partitionPaths){
			FileInputStream fis = new FileInputStream (new File(path));
			try(ObjectInputStream stream = new ObjectInputStream(fis)) {
				Element ele;
				while((ele = (Element)stream.readObject()) != null) {
					
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
}
