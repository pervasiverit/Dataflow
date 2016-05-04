package com.dataflow.scheduler;
import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.dataflow.io.InputFormat;
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
	final InputFormat inputFormat;
	
	public String getTaskId(){
		return taskId;
	}
	
	public String getJobId(){
		return jobId;
	}
	
	public Stage(final InputFormat inputFormat, final String jobId) {
		queue = new ConcurrentLinkedQueue<>();
		this.jobId = jobId;
		this.inputFormat = inputFormat;
	}
	
	protected InputFormat getInputFormat(){
		return inputFormat;
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
