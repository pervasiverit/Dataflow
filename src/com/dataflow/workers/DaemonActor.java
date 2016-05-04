package com.dataflow.workers;

import com.dataflow.messages.WorkToBeDone;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class DaemonActor extends UntypedActor{
	
	private ActorRef manager;
	private Config config;
	
	public DaemonActor(Config config) {
		this.config = config;
	}
	
	@Override
	public void preStart() throws Exception {
		manager = getContext().actorOf(Props.
				create(WorkerManager.class), "WorkerManager");
		getContext().actorOf(Props.
				create(HeartBeatActor.class, config, manager), "HeartBeatActor");
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof WorkToBeDone)
			manager.forward(msg, getContext());
		else 
			unhandled(msg);
	}

}
