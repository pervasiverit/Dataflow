package com.dataflow.nameserver;


import com.dataflow.messages.RegisterWorker;
import com.dataflow.messages.WorkRequest;
import com.dataflow.workers.HeartBeatActor.HBMessage;
import com.typesafe.config.Config;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

public class NameServerActor extends UntypedActor{

	private ActorSelection jobManager;

	public NameServerActor(Config config) {
		this.jobManager = getContext().system().actorSelection
				(config.getString("akka.actor.job-manager"));
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof HBMessage){
			HBMessage hbMsg = (HBMessage) msg;
			System.out.println(hbMsg);
		}
		else if(msg instanceof RegisterWorker || msg instanceof WorkRequest) {
			jobManager.tell(msg, getSelf());
		}
	}
	
}
