package com.dataflow.nameserver;


import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.WorkMessage;
import com.dataflow.utils.Constants;
import com.dataflow.workers.HeartBeatActor.HBMessage;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.remote.RemoteActorRef;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class NameServerActor extends UntypedActor{

	private final ActorSelection jobManager;
	private final ActorRef clusterManager;
	private final ActorSystem system = getContext().system();
	//private final Queue<? extends WorkMessage> = new LinkedList();
	
	public NameServerActor(Config config) {
		this.jobManager = system.actorSelection
				(config.getString("akka.actor.job-manager"));
		this.clusterManager = getContext().actorOf(Props.
				create(ClusterManager.class), "ClusterManager");
	}
	
	public void tryConnectingToJobManager(){
		Future<ActorRef> future = jobManager.
				resolveOne(Duration.create(5, "seconds"));
		Patterns.pipe(future, system.dispatcher()).to(getSelf());
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		//MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
		if(msg instanceof HBMessage) {
			clusterManager.tell(getSender(), getSelf());
		}
		else {
			jobManager.tell(msg, getSelf());
		}
	}
	
	public void handle(HBMessage msg){
		clusterManager.tell(getSender(), getSelf());
	}
	
	public <T extends WorkMessage> void handle(T msg){
		jobManager.tell(msg, getSelf());
	}
	
	public void handle(RemoteActorRef ref){
		
	}
	
	public void handle(Failure exception){
		tryConnectingToJobManager();
	}
}
