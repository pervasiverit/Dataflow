package com.dataflow.workers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;

public class Worker {
	public static void main(String[] args) {
		Config config = ConfigFactory.load("worker");
		ActorSystem system = ActorSystem.create("WorkerSystem", config);
		system.actorOf(Props.create(DaemonActor.class, config), "DaemonActor");
	}
}
