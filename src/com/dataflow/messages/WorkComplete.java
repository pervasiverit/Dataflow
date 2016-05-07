package com.dataflow.messages;

import java.util.Optional;

import akka.actor.ActorRef;

public class WorkComplete extends WorkMessage{
	final String path;
	final String taskId;
	
	public WorkComplete(final ActorRef actorRef, final String path, final String taskId){
		super(actorRef);
		this.path = path;
		this.taskId = taskId;
	}
	
	public String getPath(){
		return path;
	}
	
	public String getTaskId(){
		return taskId;
	}
}
