package com.dataflow.scheduler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dataflow.actors.MapActor;
import com.dataflow.edges.Edge;
import com.dataflow.io.InputFormat;
import com.dataflow.io.OutputFormat;
import com.dataflow.vertex.AbstractVertex;
import com.dataflow.vertex.AbstractVertex.VertexType;
import com.dataflow.vertex.InputVertex;
import com.dataflow.vertex.VertexList;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;

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
	private InputFormat instanceOfInputFormat;
	private OutputFormat instanceOfOutputFormat;
	protected final String jobId = UUID.randomUUID().toString();
	
	public DataFlowJob() {
		stageList = new StageList();
	}

	
	public String getJobId(){
		return jobId;
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
	 * Input Format for the file that needs to be read.
	 * 
	 * @return InputFormat
	 */
	public InputFormat getInputFormat() {
		return instanceOfInputFormat;
	}

	/**
	 * Output Format for a file that needs to be written
	 * 
	 * @return
	 */
	public OutputFormat getOutputFormat() {
		return instanceOfOutputFormat;
	}

	/**
	 * TODO: If time permits change it to a CompletableFuture.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run() throws IOException {

		
		Constructor<? extends InputFormat> cons;
		Constructor<? extends OutputFormat> oCons;
		try {
			cons = inputFormat.getConstructor(File.class);
			oCons = outputFormat.getConstructor(File.class);
		} catch (NoSuchMethodException | SecurityException err) {
			throw new RuntimeException("Error while creating an input constructor");
		}
		try {
			instanceOfInputFormat = (InputFormat) cons.newInstance(file);
			instanceOfOutputFormat = (OutputFormat) oCons.newInstance(outFile);
		} catch (Exception e) {

		}
		
		ActorSystem system = ActorSystem.create();
		ActorRef ref = system.actorOf(Props.create(MapActor.class)
				.withDispatcher("control-aware-dispatcher")
				.withRouter(new RoundRobinPool(5)));
		for(int i=0 ;i < 2; i++){
			ref.tell(stageList.get(i), ActorRef.noSender());
		}
		
		System.out.println("Finished..");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		system.shutdown();
	}

	/**
	 * Set up stages
	 * 
	 * @param io 	IO Vertex list
	 */
	public void setRoot(VertexList io) {
		for (AbstractVertex v : io) {
			stageList.add(getStage(v, new VertexList(), new PointWiseStage(this)));
		}
	}

	/**
	 * Stage is effectively final. Do not change it
	 * @param rootVertex
	 * @param vList
	 * @param stage
	 * @return
	 */
	private Stage getStage(final AbstractVertex rootVertex, final VertexList vList, final Stage stage) {
		if (rootVertex.getOutput().size() == 0) {
			vList.add(rootVertex);
			for (AbstractVertex stageVertex : vList) {
				stage.addVertexList(stageVertex);
			}
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
