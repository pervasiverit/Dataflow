package com.dataflow.scheduler;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.dataflow.utils.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Stage {

	private Queue<AbstractVertex> queue;
	
	public Stage(){
		queue = new ConcurrentLinkedQueue<>();
	}
	
	public void addVertexList(final AbstractVertex v){
		queue.add(v);
	}

	public void run() throws IOException {
		AbstractVertex abs = queue.poll();
		final int size = queue.size();
		Collector<String> collect = new Collector<>();
		abs.execute("", collect);
		for (int i = 0; i < size; ++i) {
			Collector<String> temp = new Collector<>();
			AbstractVertex mapSide = queue.poll();
			for (String s : collect) {
				mapSide.execute(s, temp);
			}
			collect = temp;
		}
	}
}
