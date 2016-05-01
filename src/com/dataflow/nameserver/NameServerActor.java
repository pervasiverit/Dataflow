package com.dataflow.nameserver;


import com.dataflow.workers.HeartBeatActor.HBMessage;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

public class NameServerActor extends UntypedActor{
	
	static class NameServerCreator implements Creator<NameServerActor>{
		private static final long serialVersionUID = 1L;

		@Override
		public NameServerActor create() throws Exception {
			return new NameServerActor();
		}
	}
	
	public static Props props(){
		return Props.create(new NameServerCreator());
	}
	
	public NameServerActor() {
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof HBMessage){
			System.out.println(((HBMessage)msg).workers);
		}
	}
	
}
