package com.dataflow.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import akka.actor.ActorRef;

public class PointWiseMap extends HashMap<ActorRef, List<String>>{

	public void addCompleted(ActorRef actorRef, String path) {
		if(containsKey(actorRef)){
			List<String> paths = get(actorRef);
			paths.add(path);
			put(actorRef, paths);
		}else{
			List<String> paths = new ArrayList<>();
			paths.add(path);
			put(actorRef, paths);
		}
		
	}
	
	

}
