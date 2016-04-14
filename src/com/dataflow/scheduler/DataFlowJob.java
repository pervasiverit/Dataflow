package com.dataflow.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.dataflow.edges.Edge;
import com.dataflow.io.InputFormat;
import com.dataflow.vertex.AbstractVertex;
import com.dataflow.vertex.VertexList;

public class DataFlowJob {

	final double bCoefficient = 0.9;
	final int nCores = Runtime.getRuntime().availableProcessors();
	final int poolSize = nCores / (1 - (int) bCoefficient);
	final ExecutorService executor = Executors.newFixedThreadPool(poolSize);
	final StageList stageList;
	final private List<Callable<Void>> stageExecute = new ArrayList<>();

	public DataFlowJob() {
		stageList = new StageList();
	}

	public void setInputFormat(Class<? extends InputFormat> class1) {
		
	}

	/**
	 *  TODO: If time permits change it to a CompletableFuture.
	 * @throws IOException
	 */
	public void run() throws IOException {
		try {
			List<Future<Void>> future = executor.invokeAll(stageExecute, 100, TimeUnit.SECONDS);
			executor.shutdown();
			//future.stream().forEach(System.out::println);
			
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set up stages 
	 * @param io	IO Vertex list
	 */
	public void start(VertexList io) {
		for (AbstractVertex v : io) {
			stageList.add(getStage(v, new VertexList(), new Stage()));
		}
	}

	/**
	 * Stage is effectively final. Do not change it
	 * 
	 * @param rootVertex
	 * @param vList
	 * @param stage
	 * @return
	 */
	private Stage getStage(final AbstractVertex rootVertex, VertexList vList, Stage stage) {
		if (rootVertex.getOutput().size()==0) {
			for (AbstractVertex stageVertex : vList) {
				stage.addVertexList(stageVertex);
				stageExecute.add(() -> {
						stage.run();
						return null;
					}
				);
			}
			return stage;
		}
		vList.add(rootVertex);
		for (Edge e : rootVertex.getOutput()) {	
			getStage(e.getRemoteVertex(), vList, stage);
		}
		return stage;
	}

}
