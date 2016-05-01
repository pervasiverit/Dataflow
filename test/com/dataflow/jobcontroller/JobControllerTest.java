package com.dataflow.jobcontroller;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dataflow.actors.JobController;
import com.dataflow.scheduler.DataFlowJob;
import com.dataflow.scheduler.PointWiseStage;
import com.dataflow.scheduler.Stage;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;

public class JobControllerTest extends TestKit{

	

	@Before
	public void init() {

	}

	static ActorSystem system = ActorSystem.create("TestSys", ConfigFactory
			.load("TestSys"));
	ActorRef echoActorRef = system.actorOf(Props.create(JobController.class));
	
	public JobControllerTest() {
		super(system);
	}

	@Test
	public void testEcho() {
		echoActorRef.tell("Hi there", super.testActor());
		expectNoMsg();
	}
	
//	@Test
//	public void testStages() {
//		List<Stage> stages = new ArrayList<>();
//		stages.add(new PointWiseStage(new DataFlowJob()));
//		final TestActorRef<JobController> ref = TestActorRef
//				.create(system, Props.create(JobController.class), "testA");
//		echoActorRef.tell(stages, super.testActor());
//		JobController controller = ref.underlyingActor();
//		assertTrue(controller.getStages() != null);
//		assertFalse(controller.getStages().size() == 2);
//	}
	
	
	
	
	
}
