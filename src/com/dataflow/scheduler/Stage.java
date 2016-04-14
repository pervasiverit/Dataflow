package com.dataflow.scheduler;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.dataflow.io.InputFormat;
import com.dataflow.utils.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Stage {

	private Queue<AbstractVertex> queue;
	private DataFlowJob job;
	
	public Stage(DataFlowJob job){
		queue = new ConcurrentLinkedQueue<>();
		this.job = job;
	}
	
	public void addVertexList(final AbstractVertex v){
		queue.add(v);
	}

	public <T> void run() throws IOException {
		InputFormat inf =job.getInputFormat();
		inf.open();
		String line = "";
		Collector<T> collector = new Collector<>();
		AbstractVertex abs = queue.poll();
		final int size = queue.size();
		while((line=(String) inf.next() )!=null){
			
			abs.execute(line, collector);
		}
		for (int i = 0; i < size; ++i) {
			Collector<T> temp = new Collector<>();
			AbstractVertex mapSide = queue.poll();
			for (T s : collector) {
				mapSide.execute(s, temp);
			}
			collector = temp;
		}
	}
	
	@Override
	public String toString(){
		return queue.toString();
	}
}
