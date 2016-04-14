import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class LookupNode {

	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create("HelloService", ConfigFactory.load("HelloService"));
		ActorRef actor = system.actorOf(Props.create(MyHello.class), "remote-worker");
		System.out.println(actor.path());
	}
	
	
	
}

class MyHello extends UntypedActor{

	@Override
	public void onReceive(Object message) throws Exception {
		if(message instanceof String){
			System.out.println(message);
		}
		
	}
	
}
