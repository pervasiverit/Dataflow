package com.dataflow.messages;

import java.util.List;
import java.util.Map;

import com.dataflow.scheduler.Stage;
import com.dataflow.utils.IntermediatePathList;

import akka.actor.ActorRef;

public class ReduceWorkToBeDone extends WorkMessage{
	
	private final Stage stage;
	private final Map<ActorRef,List<String>> actorPathMapping;
	
	private static final long serialVersionUID = 5240587869036126005L;
	
	public ReduceWorkToBeDone(final ActorRef workerRef, 
							  final Stage stage, 
							  final Map<ActorRef,List<String>> actorRef
							  ) {
		super(workerRef);
		this.stage = stage;
		this.actorPathMapping = actorRef;
	}
	
	public Stage getStage(){
		return stage;
	}

	public Map<ActorRef,List<String>> getActorPathMapping(){
		return actorPathMapping;
	}
	

}
