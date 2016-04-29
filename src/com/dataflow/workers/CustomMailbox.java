package com.dataflow.workers;

import com.dataflow.workers.WorkerActor.WorkerState;
import com.typesafe.config.Config;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedStablePriorityMailbox;

public class CustomMailbox {
	
	static class ManagerMailbox extends UnboundedStablePriorityMailbox {

		public ManagerMailbox(ActorSystem.Settings settings, Config config) {
			super(new PriorityGenerator() {
				@Override
				public int gen(Object message) {
					if (message instanceof WorkerState)
						return 0;
					else if (message.equals("lowpriority"))
						return 2;
					else if (message.equals(PoisonPill.getInstance()))
						return 3;
					else
						return 1;
				}
			});
		}
		
	}
	
	static class WorkerMailbox extends UnboundedStablePriorityMailbox {
		
		public WorkerMailbox(ActorSystem.Settings settings, Config config) {
			super(new PriorityGenerator() {
				@Override
				public int gen(Object message) {
					if (message.equals("highpriority"))
						return 0;
					else if (message.equals("lowpriority"))
						return 2;
					else if (message.equals(PoisonPill.getInstance()))
						return 3;
					else
						return 1;
				}
			});
		}
		
	}
	
}
