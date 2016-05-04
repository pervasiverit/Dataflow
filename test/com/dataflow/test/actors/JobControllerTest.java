package com.dataflow.test.actors;

import org.junit.Before;
import org.junit.Test;

import com.dataflow.actors.JobController;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
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
	
	@Test
	public void testStages() {
		
	}
	
	
	
	
	
}
