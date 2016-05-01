package com.dataflow.messages;

import java.util.Optional;

import akka.actor.ActorRef;

public class MapWorkComplete extends WorkComplete{
	
	public MapWorkComplete(final ActorRef actorRef, final Optional<String> path, final String taskId) {
		super(actorRef, path, taskId);
	}

}
