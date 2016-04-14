

import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class AHello {

	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create(
				"HelloAppService",ConfigFactory.load().getConfig("HelloAppService"));
		final String path = "akka.tcp://HelloService@127.0.0.1:2552/user/remote-worker";
		ActorSelection actorSelection = system.actorSelection(path);
		actorSelection.tell("Suman",null);
	}
}
