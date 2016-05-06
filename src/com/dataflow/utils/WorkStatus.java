package com.dataflow.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import com.dataflow.messages.WorkComplete;
import com.dataflow.messages.WorkMessage;
import com.dataflow.messages.WorkToBeDone;
import com.dataflow.scheduler.Stage;

import akka.actor.ActorRef;

public class WorkStatus {

	// pending work
	private ConcurrentLinkedDeque<Stage> pendingWork = new ConcurrentLinkedDeque<>();

	// Job in progress
	private Map<ActorRef, Stage> workInProgress = new HashMap<>();

	// accepted work ids.
	private HashSet<String> acceptedWorkIds = new HashSet<>();

	// completed work ids.
	private HashSet<String> comepletedWorkIds = new HashSet<>();

	public boolean hasWork() {
		return !pendingWork.isEmpty();
	}

	public Stage next() {
		return pendingWork.getFirst();
	}

	public WorkStatus() {

	}

	public WorkStatus getInstance(WorkStatus curr, Object message) throws Exception {
		return (WorkStatus) ConstructorUtils.invokeConstructor(this.getClass(), curr, message);
	}

	public WorkStatus(WorkStatus currentWorkState, WorkToBeDone workToBeDone) {
		ConcurrentLinkedDeque<Stage> tmp_pendingWork = new ConcurrentLinkedDeque<>(currentWorkState.pendingWork);
		Map<ActorRef, Stage> tmp_workInProgress = new HashMap<>(currentWorkState.workInProgress);
		Stage stage = tmp_pendingWork.removeFirst();
		if (!stage.getTaskId().equals(workToBeDone.getStage().getTaskId())) {
			throw new IllegalArgumentException("Error expected  ");
		}
		tmp_workInProgress.put(workToBeDone.getActorRef(), stage);
		workInProgress = tmp_workInProgress;
		acceptedWorkIds = new HashSet<String>(currentWorkState.acceptedWorkIds);
		comepletedWorkIds = new HashSet<String>(currentWorkState.comepletedWorkIds);
		pendingWork = tmp_pendingWork;
	}

	public WorkStatus(WorkStatus curr, WorkComplete message) {
		Map<ActorRef, Stage> tmp_workInProgress = new HashMap<>(curr.workInProgress);
		HashSet<String> tmp_doneWorkIds = new HashSet<String>(curr.comepletedWorkIds);
		tmp_workInProgress.remove(message.getTaskId());
		tmp_doneWorkIds.add(message.getTaskId());
		workInProgress = tmp_workInProgress;
		acceptedWorkIds = new HashSet<String>(curr.acceptedWorkIds);
		comepletedWorkIds = tmp_doneWorkIds;
		pendingWork = new ConcurrentLinkedDeque<>(curr.pendingWork);
	}
	

	  public WorkStatus(WorkStatus curr, Stage workAccepted) {
	    ConcurrentLinkedDeque<Stage> tmp_pendingWork = new ConcurrentLinkedDeque<>(curr.pendingWork);
	    HashSet<String> tmp_acceptedWorkIds = new HashSet<String>(curr.acceptedWorkIds);
	    tmp_pendingWork.addLast(workAccepted);
	    tmp_acceptedWorkIds.add(workAccepted.getTaskId());
	    workInProgress = new HashMap<>(curr.workInProgress);
	    acceptedWorkIds = tmp_acceptedWorkIds;
	    comepletedWorkIds = new HashSet<String>(curr.comepletedWorkIds);
	    pendingWork = tmp_pendingWork;
	  }

}
