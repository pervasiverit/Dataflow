package com.dataflow.workers;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.Iterable;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.elements.Element;
import com.dataflow.messages.ReadPartition;
import com.dataflow.messages.ReduceWorkToBeDone;
import com.dataflow.scheduler.CrossProductStage;
import com.dataflow.utils.Constants;
import com.dataflow.utils.ElementList;
import com.dataflow.workers.WorkerActor.WorkerState;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.util.ByteString;
import akka.util.Timeout;
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
		getContext().parent().tell(WorkerState.BUSY, workerActor);
		MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
	}
	
	public void handle(ReduceWorkToBeDone workToDo) throws Exception {
		Map<ActorRef, List<String>> mappers = workToDo.getActorPathMapping();
		List<Future<Object>> futures = new ArrayList<>();
		Timeout timeout = new Timeout(Duration.create(10, "seconds"));
		mappers.forEach((mapper, partitionPaths)->partitionPaths.forEach(
				(partitionPath)->futures.add(Patterns.ask(mapper, 
						new ReadPartition(getSelf(), partitionPath), timeout))));
		
		Future<Iterable<Object>> future = Futures.sequence(futures, system.dispatcher());
		Iterable<Object> iterable = Await.result(future, timeout.duration());
		ElementList elementsList = process(iterable);
		CrossProductStage stage = (CrossProductStage) workToDo.getStage();
		stage.setElementList(elementsList);
		
		workerActor.forward(workToDo, getContext());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ElementList process(Iterable<Object> iterable) throws Exception {
		Iterator<Object> it = iterable.iterator();
		
		ElementList list = new ElementList();
		Element ele;
		while(it.hasNext()) {
			ByteString file = (ByteString) it.next();
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
