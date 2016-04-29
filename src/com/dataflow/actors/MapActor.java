package com.dataflow.actors;
import java.io.IOException;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.scheduler.Stage;

import akka.actor.UntypedActor;

public class MapActor extends UntypedActor{

	
	@Override
	public void onReceive(Object message) throws Exception {
		MethodUtils.invokeMethod(this, "handler",message);
	}
	
	public void handler(Stage stg){
		try {
			stg.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
