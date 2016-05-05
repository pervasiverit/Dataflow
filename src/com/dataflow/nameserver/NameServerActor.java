package com.dataflow.nameserver;


import com.dataflow.workers.HeartBeatActor.HBMessage;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class NameServerActor extends UntypedActor{

	private ActorSelection jobManager;
	private ActorRef clusterManager;

	public NameServerActor(Config config) {
		this.jobManager = getContext().system().actorSelection
				(config.getString("akka.actor.job-manager"));
		this.clusterManager = getContext().actorOf(Props.
				create(ClusterManager.class), "ClusterManager");
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof HBMessage) {
			clusterManager.tell(getSender(), getSelf());
		}
		else {
			jobManager.tell(msg, getSelf());
		}
	}
	
}
