import com.dataflow.actors.JobController;
import com.dataflow.actors.WorkerActor;
import com.dataflow.actors.WorkerExec;
import com.dataflow.messages.RegisterWorker;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class StartDaemon {
	final static int jobPort = 5919;
	final static ActorPath path = ActorPaths.fromString("akka.tcp://JobController@127.0.0.1:5919/user/store");
	final static int workerPort = 5929;

	public static void main(String[] args) throws Exception {
		start();
		Thread.sleep(1000);
		
	}

	private static void start() {
		final Config conf = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + jobPort)
				.withFallback(ConfigFactory.load());
		final ActorSystem system = ActorSystem.create("JobController");
		final ActorRef actorJ = system.actorOf(Props.create(JobController.class), "persistent");
		actorJ.tell("heelllo", ActorRef.noSender());
	
		final ActorSystem wsystem = ActorSystem.create("WorkerSystem");
		final Props props = Props.create(WorkerActor.class);
		final ActorRef actor = wsystem.actorOf(Props.create(WorkerExec.class, props, actorJ)
												.withDispatcher("control-aware-dispatcher"));
		actor.tell(new RegisterWorker(actor), ActorRef.noSender());
		
	}

}
