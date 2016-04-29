package com.dataflow.scheduler;
import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.dataflow.vertex.AbstractVertex;

/***
 * TODO: Very bad idea to supress "unchecked" exceptions.
 * Fix it when time ASAP.
 * @author sumanbharadwaj
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class Stage implements Serializable {
	private static final long serialVersionUID = 4123829129686802172L;
	protected Queue<AbstractVertex> queue;
	protected String jobId;
	
	protected String taskId = UUID.randomUUID().toString();
	
	
	public String getTaskId(){
		return taskId;
	}
	
	public String getJobId(){
		return jobId;
	}
	
	final protected transient DataFlowJob job;

	public Stage(DataFlowJob job) {
		queue = new ConcurrentLinkedQueue<>();
		this.job = job;
		this.jobId = job.getJobId();
	}

	public void addVertexList(final AbstractVertex v) {
		queue.add(v);
	}

	public abstract <T> void run() throws IOException;
	

	@Override
	public String toString() {
		return queue.toString();
	}
}
