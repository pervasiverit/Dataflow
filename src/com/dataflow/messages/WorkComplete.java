package com.dataflow.messages;

import java.util.Optional;

import akka.actor.ActorRef;

public class WorkComplete extends WorkMessage{
	final Optional<String> path;
	
	public WorkComplete(final ActorRef actorRef, final Optional<String> path){
		super(actorRef);
		this.path = path;
	}
	
	public Optional<String> getPath(){
		return path;
	}
}
