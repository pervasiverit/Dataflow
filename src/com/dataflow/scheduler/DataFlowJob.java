package com.dataflow.scheduler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;

import com.dataflow.edges.Edge;
import com.dataflow.io.InputFormat;
import com.dataflow.io.OutputFormat;
import com.dataflow.vertex.AbstractVertex;
import com.dataflow.vertex.AbstractVertex.VertexType;
import com.dataflow.vertex.VertexList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;

/**
 * Define the job class. If the job
 * 
 * @author sumanbharadwaj
 *
 */
@SuppressWarnings("rawtypes")
public class DataFlowJob {

	final StageList stageList;
	private Class<? extends InputFormat> inputFormat;
	private File file;
	private File outFile;
	private Class<? extends OutputFormat> outputFormat;
	protected final String jobId = UUID.randomUUID().toString();

	public DataFlowJob() {
		stageList = new StageList();
	}

	public String getJobId() {
		return jobId;
	}
	
	private Class<? extends InputFormat> getInputFormatClass(){
		return inputFormat;
	}
	
	private Class<? extends OutputFormat> getOutputFormatClass(){
		return outputFormat;
	}

	/**
	 * Set Input Format class name
	 * 
	 * @return InputFormat
	 */
	public void setInputFormat(Class<? extends InputFormat> inf) {
		this.inputFormat = inf;
	}
	
	/**
	 * TODO: If time permits change it to a CompletableFuture.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run() throws IOException {
		ActorSystem actorSystem = ActorSystem.create();
		Config conf = ConfigFactory.load("application");
		ActorSelection actor = actorSystem.actorSelection(conf.getString("job-manager"));
		actor.tell(stageList, ActorRef.noSender());
		System.out.println("Finished..");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		// system.shutdown();
	}

	Queue<VertexList> queue = new ArrayDeque<>();

	/**
	 * Set up stages
	 * 
	 * @param io
	 *            IO Vertex list
	 */
	public void setRoot(VertexList io) {
		queue.add(io);
		while (!queue.isEmpty()) {
			for (AbstractVertex vertex : queue.poll()) {
				if (vertex.getVertexType() == VertexType.POINT_WISE){
					Stage stg = new PointWiseStage(getInputFormatClass(), jobId, file);
					stageList.add(getStage(vertex, new VertexList(), stg));
				}
				else {
					Stage stg  = new CrossProductStage(getOutputFormatClass(), jobId, outFile);
					stageList.add(getStage(vertex, new VertexList(), stg));
				}
			}
		}
		System.out.println(stageList.size());
	}

	/**
	 * BAD:
	 * 
	 * Stage is effectively final. Do not change it
	 * 
	 * @param rootVertex
	 * @param vList
	 * @param stage
	 * @return
	 */
	private Stage getStage(final AbstractVertex rootVertex, final VertexList vList, final Stage stage) {
		if (rootVertex.getVertexType() == VertexType.SHUFFLE)
			manageShuffle(rootVertex, vList, stage);
		vList.add(rootVertex);

		for (Edge e : rootVertex.getOutput()) {
			getStage(e.getRemoteVertex(), vList, stage);
		}
		return stage;
	}

	private boolean visited = false;

	private Stage manageShuffle(AbstractVertex rootVertex, VertexList vList, Stage stage) {
		vList.add(rootVertex);
		for (AbstractVertex stageVertex : vList) {
			stage.addVertexList(stageVertex);
		}
		if (!visited) {
			createVertexListAndToQueue(rootVertex);
		}
		vList.remove(rootVertex);
		return stage;
	}

	private void createVertexListAndToQueue(AbstractVertex rootVertex) {
		VertexList v = new VertexList();
		rootVertex.getOutput().stream().forEach(e -> v.add(e.getRemoteVertex()));
		queue.add(v);
		visited = true;
	}

	/**
	 * Set Input Path of the file in the Job
	 * 
	 * @param filePath
	 */
	public void setInputPath(String filePath) {
		this.file = new File(filePath);

	}

	public void setOutputPath(String outPath) {
		this.outFile = new File(outPath);
	}

	/**
	 * configure the user specified output format.
	 * 
	 * @param of
	 */
	public void setOutputFormat(Class<? extends OutputFormat> of) {
		this.outputFormat = of;
	}

}
