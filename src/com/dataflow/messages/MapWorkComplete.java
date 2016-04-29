package com.dataflow.messages;

import java.util.Optional;

import akka.actor.ActorRef;

public class MapWorkComplete extends WorkComplete{

	public MapWorkComplete(ActorRef actorRef, Optional<String> path) {
		super(actorRef, path);
	}

}
