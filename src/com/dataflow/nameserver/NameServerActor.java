package com.dataflow.nameserver;


import com.dataflow.workers.HeartBeatActor.HBMessage;
import com.typesafe.config.Config;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

public class NameServerActor extends UntypedActor{
	
<<<<<<< Updated upstream
	static class NameServerCreator implements Creator<NameServerActor>{
		private static final long serialVersionUID = 1L;
=======
	private ActorSelection jobManager;
>>>>>>> Stashed changes

	public NameServerActor(Config config) {
		this.jobManager = getContext().system().actorSelection
				(config.getString("akka.actor.job-manager"));
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof HBMessage){
			HBMessage hbMsg = (HBMessage) msg;
			hbMsg.workReq.ifPresent((workReq)->jobManager.tell(workReq, getSelf()));
		}
	}
	
}
