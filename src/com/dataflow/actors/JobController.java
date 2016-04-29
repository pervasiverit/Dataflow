package com.dataflow.actors;

import static com.dataflow.utils.Constants.HANDLER;
import static com.dataflow.utils.Constants.QUEUE_SIZE;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.MapWorkComplete;
import com.dataflow.messages.WorkRequest;
import com.dataflow.messages.WorkToBeDone;
import com.dataflow.scheduler.CrossProductStage;
import com.dataflow.scheduler.PointWiseStage;
import com.dataflow.scheduler.Stage;
import com.dataflow.scheduler.StageList;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class JobController extends UntypedActor {

	final Queue<Stage> stages = new ArrayBlockingQueue<Stage>(QUEUE_SIZE);
	final HashMap<ActorRef, StageList> running = new HashMap<>();
	final HashMap<ActorRef, String> completedPointWiseTasks = new HashMap<>();

	@Override
	public void onReceive(Object message) throws Exception {
		MethodUtils.invokeMethod(this, HANDLER, message);
	}

	public void handle(PointWiseStage stage) {
		final String jobID = stage.getJobId();
		stages.add(stage);
	}

	public void handle(CrossProductStage stage) {

	}

	/**
	 * Handler for the map work completed message. Worker sends a path and its
	 * ActorRef. Store it in a hashmap
	 * 
	 * @param work
	 */
	public void handle(MapWorkComplete work) {
		String path = work.getPath().orElse("");
		completedPointWiseTasks.put(work.getActorRef(), path);
		running.remove(work.getActorRef());
	}

	/**
	 * Handle the work request from either workers or nameserver
	 * 
	 * @param work
	 */
	public void handle(WorkRequest work) throws Exception{
		ActorRef ref = work.getActorRef();
		WorkToBeDone toBeDone = (WorkToBeDone) MethodUtils
				.invokeMethod(this, "getWorkToBeDone", stages.poll(), work);
		ref.tell(toBeDone, ActorRef.noSender());
	}

	public WorkToBeDone getWorkToBeDone(PointWiseStage stage, WorkRequest work) {
		return new WorkToBeDone(getSelf(), stages.poll(), Optional.ofNullable(null));
	}

	public WorkToBeDone getWorkToBeDone(CrossProductStage stage, WorkRequest work) {
		String path = completedPointWiseTasks.get(work.getActorRef());
		return new WorkToBeDone(getSelf(), stages.poll(),Optional.of(path));
	}

	/**
	 * FOR THE LOVE OF GOD; CHECKED EXCEPTIONS LEAVE ME ALONE !!!!!
	 * 
	 * @param List<Stage>
	 *            stage
	 */
	public void handle(List<Stage> stage) {
		stage.stream().forEach(param -> {
			try {
				MethodUtils.invokeMethod(this, HANDLER, param);
			} catch (Exception e) {

			}
		});
	}

}
