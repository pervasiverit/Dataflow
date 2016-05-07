package com.dataflow.test.actors;

import java.io.File;

import org.junit.Test;

import com.dataflow.actors.WorkerActor;
import com.dataflow.actors.WorkerExec;
import com.dataflow.io.TextFileInputFormat;
import com.dataflow.messages.RegisterWorker;
import com.dataflow.messages.WorkIsReady;
import com.dataflow.messages.WorkToBeDone;
import com.dataflow.scheduler.PointWiseStage;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Function;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActor;
import akka.testkit.TestActor.AutoPilot;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;

public class WorkerExecTest extends TestKit {

	
	static ActorSystem system = ActorSystem.create("TestSys",
			ConfigFactory.parseString("akka.loggers = [akka.testkit.TestEventListener]"));

	
	public WorkerExecTest() {
		super(system);
	}

	@Test
	public void testRegisterWorker() {
		new JavaTestKit(system) {
			{
				final JavaTestKit probe = new JavaTestKit(system);
				probe.setAutoPilot(new TestActor.AutoPilot() {
					@Override
					public AutoPilot run(ActorRef sender, Object message) {
						if (message instanceof RegisterWorker) {
							sender.tell(new WorkIsReady(sender), sender);
						}
						return noAutoPilot();
					}
				});
				Function<ActorRefFactory, ActorRef> maker = new Function<ActorRefFactory, ActorRef>() {
					@Override
					public ActorRef apply(ActorRefFactory f) throws Exception {
						return ActorRef.noSender();
					}
				};
				final Props props = Props.create(WorkerActor.class);
				ActorRef workerActor = system.actorOf(Props.create(WorkerExec.class, maker, probe.getRef()));
				workerActor.tell(new RegisterWorker(getRef()), getRef());
				probe.expectMsgClass(RegisterWorker.class);
			}
		};
	}
//
//	@Test
//	public void testWorkToBeDoneWorker() {
//		new JavaTestKit(system) {
//			{
//				final TestProbe probe = new TestProbe(system);
//				Function<ActorRefFactory, ActorRef> maker = new Function<ActorRefFactory, ActorRef>() {
//					@Override
//					public ActorRef apply(ActorRefFactory f) throws Exception {
//						return probe.ref();
//					}
//				};
//				ActorRef workerActor = system.actorOf(Props.create(WorkerExec.class, maker, ActorRef.noSender()));
//				File file = new File("common.txt");
//				PointWiseStage stage = new PointWiseStage(new TextFileInputFormat(file,0), "abc", file);
//				probe.send(workerActor, new WorkToBeDone(ActorRef.noSender(), stage, ""));
//				probe.expectMsgClass(WorkToBeDone.class);
//				TestActorRef<WorkerExec> parent = TestActorRef.create(
//						system, Props.create(WorkerExec.class, maker, ActorRef.noSender()));
//			}
//		};
//	}
}
