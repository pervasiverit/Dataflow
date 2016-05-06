package com.dataflow.workers;

import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Worker {
	public final int numOfCores;
	
	public Worker() {
		this.numOfCores = Runtime.getRuntime().availableProcessors();
	}
	
	public static void main(String[] args) {
		Worker worker = new Worker();
		Config config = ConfigFactory.load("worker");
		ActorSystem system = ActorSystem.create("WorkerSystem", config);
		
		List<ActorRef> daemons = new ArrayList<>();
		for(int i=0; i<worker.numOfCores; i++){
			daemons.add(system.actorOf(Props
					.create(DaemonActor.class), "DaemonActor"+i));
		}
		
		system.actorOf(Props.
				create(HeartBeatActor.class, config, daemons), "HeartBeatActor");
		
	}
}
