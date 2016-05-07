package com.dataflow.workers;

import java.util.List;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.ConnectionComplete;
import com.dataflow.messages.Message;
import com.dataflow.utils.Constants;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Status.Failure;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.remote.RemoteActorRef;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class HeartBeatActor extends UntypedActor{
	
	static public class HBMessage implements Message {

		private static final long serialVersionUID = 1L;
		private static final HBMessage INSTANCE = new HBMessage();
		
		private HBMessage(){}
		
		public static HBMessage getInstance(){
			return INSTANCE;
		}
	}
	
	private ActorSystem system;
	private ActorSelection nameServer;
	private List<ActorRef> daemons;
	
	public HeartBeatActor(Config config, List<ActorRef> daemons) {
		this.system = getContext().system();
		this.nameServer = system.actorSelection
				(config.getString("akka.actor.name-server"));
		this.daemons = daemons;
	}
	
	public void tryConnectingToNameserver(){
		Future<ActorRef> future = nameServer.
				resolveOne(Duration.create(5, "seconds"));
		Patterns.pipe(future, system.dispatcher()).to(getSelf());
	}
	
	@Override
	public void preStart() throws Exception {
		tryConnectingToNameserver();
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
	}
	
	public void handle(RemoteActorRef ref){
		daemons.forEach((deamon)->deamon.
				tell(new ConnectionComplete(ref), getSelf()));
		
		system.scheduler().schedule(Duration.Zero(), 
				Duration.create(5, "seconds"), ref, HBMessage.getInstance(),
				system.dispatcher(), getSelf());
	}
	
	public void handle(Failure exception){
		tryConnectingToNameserver();
	}
}
