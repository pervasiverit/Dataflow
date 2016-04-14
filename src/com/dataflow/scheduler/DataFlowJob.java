package com.dataflow.scheduler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
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

/**
 * Define the job class. If the job 
 * @author sumanbharadwaj
 *
 */
public class DataFlowJob {

	final double bCoefficient = 0.9;
	final int nCores = Runtime.getRuntime().availableProcessors();
	final int poolSize = (int) ( nCores / (1.0 -  bCoefficient));
	final ExecutorService executor = Executors.newFixedThreadPool(poolSize);
	final StageList stageList;
	final private List<Callable<Void>> stageExecute = new ArrayList<>();
	private Class<? extends InputFormat> inputFormat;
	private InputFormat instanceOfInputFormat;
	private  File file;
	
	public DataFlowJob() {
		stageList = new StageList();
	}
	

	public void setInputFormat(Class<? extends InputFormat> inf) {
		this.inputFormat = inf;
	}
	
	public InputFormat getInputFormat(){
		return instanceOfInputFormat;
	}

	/**
	 * TODO: If time permits change it to a CompletableFuture.
	 * 
	 * @throws IOException
	 */
	public void run() throws IOException {
		
		Constructor<? extends InputFormat> cons;
		try {
			cons = inputFormat.getConstructor(File.class);
		} catch (NoSuchMethodException | SecurityException err) {
			throw new RuntimeException("Error while creating an input constructor");
		}
		try {
			instanceOfInputFormat = (InputFormat) cons.newInstance(file);
		} catch (Exception e) {
			
		}
		
		try {
			List<Future<Void>> future = executor.invokeAll(stageExecute, 10000, TimeUnit.SECONDS);
			executor.shutdown();
			// future.stream().forEach(System.out::println);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set up stages
	 * 
	 * @param io
	 *            IO Vertex list
	 */
	public void start(VertexList io) {
		for (AbstractVertex v : io) {
			stageList.add(getStage(v, new VertexList(), new Stage(this)));
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
		if (rootVertex.getOutput().size() == 0) {
			vList.add(rootVertex);
			for (AbstractVertex stageVertex : vList) {
				stage.addVertexList(stageVertex);
			}
			stageExecute.add(() -> {
				stage.run();
				return null;
			});
			vList.remove(rootVertex);
			return stage;
		}
		vList.add(rootVertex);
		for (Edge e : rootVertex.getOutput()) {
			getStage(e.getRemoteVertex(), vList, stage);
			
		}
		return stage;
	}

	/**
	 * Set Input Path of the file in the Job
	 * @param filePath
	 */
	public void setInputPath(String filePath) {
		this.file = new File(filePath);
		
	}

}
