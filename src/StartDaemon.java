import java.util.ArrayList;
import java.util.List;

import com.dataflow.actors.JobController;
import com.dataflow.scheduler.DataFlowJob;
import com.dataflow.scheduler.PointWiseStage;
import com.dataflow.scheduler.Stage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class StartDaemon {
	
	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create();
		ActorRef ref = system.actorOf(Props.create(JobController.class)
						.withDispatcher("control-aware-dispatcher")
						);
		
		ref.tell("Heelllo", ActorRef.noSender());
		List<Stage> stages = new ArrayList<>(); 
		stages.add(new PointWiseStage(new DataFlowJob()));
		ref.tell(stages, ActorRef.noSender());
	}

}
