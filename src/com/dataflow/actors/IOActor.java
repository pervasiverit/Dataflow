package com.dataflow.actors;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.io.Collector;
import com.dataflow.vertex.AbstractVertex;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinPool;

public class IOActor extends UntypedActor {

	ActorRef ref;
	final double bCoefficient = 0.1;
	final int nCores = Runtime.getRuntime().availableProcessors();
	final int poolSize = (int) (nCores / (1.0 - bCoefficient));
	

	@Override
	public void preStart() throws Exception {
		ref = getContext().actorOf(Props.create(WorkerActor.class)
				.withRouter(new RoundRobinPool(poolSize)));
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MethodUtils.invokeMethod(this, "handleMessage", message);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void handleMessage(AbstractVertex ioVertex) {
		CompletableFuture<Collector> future = CompletableFuture.supplyAsync(() -> {
			Collector collector = new Collector("asd");
			try {
				ioVertex.execute("", collector);
			} catch (IOException e) {

			}
			return collector;
		});
		future.thenAcceptAsync(e -> {
			for(Object obj : e){
				ref.tell(obj, getSelf());
			}
		});
	}

	public void handleMessage(String message) {
		System.out.println("Hello..");
	}

}
