package com.dataflow.workers;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.elements.Element;
import com.dataflow.messages.ReadPartition;
import com.dataflow.messages.ReduceWorkToBeDone;
import com.dataflow.scheduler.CrossProductStage;
import com.dataflow.utils.Constants;
import com.dataflow.utils.ElementList;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.util.ByteString;
import akka.util.Timeout;
import scala.collection.immutable.Iterable;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class CopyPartitionActor extends UntypedActor {

	private final ActorRef workerActor;
	private final ActorSystem system = getContext().system();
	
	public CopyPartitionActor(ActorRef ref) {
		this.workerActor = ref;
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		 MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
	}
	
	@SuppressWarnings({ "unchecked" })
	public void handle(ReduceWorkToBeDone workToDo) throws Exception {
		Map<ActorRef, List<String>> mappers = workToDo.getActorPathMapping();
		List<Future<Object>> futures = new ArrayList<>();
		Timeout timeout = new Timeout(Duration.create(10, "seconds"));
		mappers.forEach((mapper, partitionPaths)->partitionPaths.forEach(
				(partitionPath)->futures.add(Patterns.ask(mapper, 
						new ReadPartition(getSelf(), partitionPath), timeout))));
		Iterable<ByteString> iterable = (Iterable<ByteString>) Await.
				result(Futures.sequence(futures, system.dispatcher()), 
						timeout.duration());
		ElementList elementsList = process(iterable);
		CrossProductStage stage = (CrossProductStage) workToDo.getStage();
		stage.setElementList(elementsList);
		
		workerActor.forward(workToDo, getContext());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ElementList process(Iterable<ByteString> iterable) throws Exception {
		ByteString[] partitionFiles = new ByteString[iterable.size()];
		iterable.copyToArray(partitionFiles);
		
		ElementList list = new ElementList();
		Element ele;
		for(ByteString file : partitionFiles){
			ObjectInputStream in = new ObjectInputStream(new 
					ByteArrayInputStream(file.toArray()));
			while((ele = (Element)in.readObject()) != null) {
				list.add(ele);
			}
		}
		
		Collections.sort(list);
		return list;
	}
	
}
