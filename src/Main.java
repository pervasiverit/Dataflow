import java.io.IOException;

import com.dataflow.builder.BuilderException;
import com.dataflow.builder.DataflowBuilder;
import com.dataflow.io.TextFileInputFormat;
import com.dataflow.scheduler.DataFlowJob;
import com.dataflow.utils.ConnectorType;
import com.dataflow.vertex.VertexList;

public class Main {

	public static void main(String [] args) throws BuilderException, IOException {
		DataFlowJob job = new DataFlowJob();
		job.setInputFormat(TextFileInputFormat.class);
		job.setInputPath("common.conf");
		DataflowBuilder builder = new DataflowBuilder();
		VertexList v1 = builder.createVertexSet(Hello.class, 3);
		VertexList v2 = builder.createVertexSet(Multiple.class, 3);
		builder.mapPointWise(v1, v2, ConnectorType.FILE);
		//job.addToJobQueue(io);
		job.start(v1);
		job.run();
	}
}
