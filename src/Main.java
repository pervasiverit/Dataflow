import java.io.File;
import java.io.IOException;

import com.dataflow.builder.BuilderException;
import com.dataflow.builder.DataflowBuilder;
import com.dataflow.io.TextFileInputFormat;
import com.dataflow.scheduler.DataFlowJob;
import com.dataflow.utils.ConnectorType;
import com.dataflow.vertex.VertexList;

public class Main {

	public static void main(String[] args) throws BuilderException, IOException {
		DataFlowJob job = new DataFlowJob();
		job.setInputFormat(TextFileInputFormat.class);
		DataflowBuilder builder = new DataflowBuilder();
		VertexList io = builder.createInputVertex(2, new File("common.conf"), TextFileInputFormat.class);
		VertexList v1 = builder.createVertexSet(new Hello(), 2);
		builder.mapPointWise(io, v1, ConnectorType.FILE);
		//job.addToJobQueue(io);
		job.start(io);
		job.run();
	}
}
