package com.dataflow.nameserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;

public class NameServer {
	public static void main(String[] args) {
		Config config = ConfigFactory.load("nameserver");
		ActorSystem system = ActorSystem.create("NameServer", config);
		system.actorOf(NameServerActor.props(), "NameServerActor");
	}
}
