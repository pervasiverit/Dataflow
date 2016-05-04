package com.dataflow.workers;

import java.util.Optional;

import com.dataflow.messages.Message;
import com.dataflow.messages.WorkRequest;
import com.typesafe.config.Config;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.UntypedActor;
import scala.concurrent.duration.Duration;

public class HeartBeatActor extends UntypedActor{
	
	static public class HBMessage implements Message{

		private static final long serialVersionUID = 1L;
		public Optional<WorkRequest> workReq;
		
		public HBMessage(){}
		
		public HBMessage(Optional<WorkRequest> request) {
			this.workReq = request;
		}
	}
	
	private ActorSystem system;
	private ActorSelection nameServer;
	private ActorRef workerManager;
	
	public HeartBeatActor(Config config, ActorRef manager) {
		this.system = getContext().system();
		this.nameServer = system.actorSelection
				(config.getString("akka.actor.name-server"));
		this.workerManager = manager;
	}
	
	@Override
	public void preStart() throws Exception {
		nameServer.tell(new Identify("Hello"), getSelf());
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if (msg instanceof ActorIdentity){
			system.scheduler().schedule(Duration.Zero(), 
					Duration.create(5, "seconds"), workerManager, 
					new HBMessage(), system.dispatcher(), getSelf());
		}
		else if (msg instanceof HBMessage) {
			nameServer.tell(msg, getSelf());
		}
	}

}
