package com.dataflow.messages;

import java.util.Optional;

import akka.actor.ActorRef;

public class WorkComplete extends WorkMessage{
	final Optional<String> path;
	final String taskId;
	
	public WorkComplete(final ActorRef actorRef, final Optional<String> path, final String taskId){
		super(actorRef);
		this.path = path;
		this.taskId = taskId;
	}
	
	public Optional<String> getPath(){
		return path;
	}
	
	public String getTaskId(){
		return taskId;
	}
}
