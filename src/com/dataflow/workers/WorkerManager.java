package com.dataflow.workers;

import java.util.ArrayList;
import java.util.List;

import com.dataflow.workers.HeartBeatActor.HBMessage;
import com.dataflow.workers.WorkerActor.WorkerState;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.routing.ActorRefRoutee;
import akka.routing.Broadcast;
import akka.routing.Routee;
import akka.routing.Router;
import akka.routing.SmallestMailboxRoutingLogic;

public class WorkerManager extends UntypedActor{
	
	public final int numOfCores;
	private Router router;
	private List<ActorRef> freeWorkers;
	
	public WorkerManager() {
		this.numOfCores = Runtime.getRuntime().availableProcessors();
		this.freeWorkers = new ArrayList<>();
	}
	
	@Override
	public void preStart() throws Exception {
		List<Routee> workers = createWorkers(numOfCores);
		router = new Router(new SmallestMailboxRoutingLogic(), workers);
		router.route(new Broadcast("Hello"), getSelf());
	}
	
	private List<Routee> createWorkers(int numOfCores) {
		List<Routee> routees = new ArrayList<Routee>();
		for(int i=0; i<numOfCores; i++){
			ActorRef worker = createWorkerActor();
		    routees.add(new ActorRefRoutee(worker));
		}
		
		return routees;
	}
	
	private ActorRef createWorkerActor(){
		ActorRef worker = getContext().actorOf(Props.create(WorkerActor.class)
				.withDispatcher("pool-dispatcher"));
		freeWorkers.add(worker);
	    getContext().watch(worker);
	    return worker;
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println(getSender()+" "+msg);
	    if(msg instanceof HBMessage) {
	    	getSender().tell(new HBMessage(freeWorkers), getSelf());
	    } 
	    else if(msg instanceof WorkerState) {
	    	WorkerState state = (WorkerState) msg;
	    	if(state == WorkerState.BUSY)
	    		freeWorkers.remove(getSender());
	    	else
	    		freeWorkers.add(getSender());
	    }
	    else if (msg instanceof Terminated) {
	    	ActorRef terminatedWorker = ((Terminated) msg).actor();
	        router = router.removeRoutee(terminatedWorker);
	        freeWorkers.remove(terminatedWorker);
	        ActorRef worker = createWorkerActor();
	        router = router.addRoutee(new ActorRefRoutee(worker));
	    }
	    else {
	    	unhandled(msg);
	    }
	}

}
