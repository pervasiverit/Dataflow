import java.io.IOException;

import com.dataflow.builder.BuilderException;
import com.dataflow.builder.DataflowBuilder;
import com.dataflow.io.TextFileInputFormat;
import com.dataflow.io.TextFileOutputFormat;
import com.dataflow.scheduler.DataFlowJob;
import com.dataflow.utils.ConnectorType;
import com.dataflow.vertex.VertexList;

public class Main {

	public static void main(String [] args) throws BuilderException, IOException {

		DataFlowJob job = new DataFlowJob();
		job.setInputFormat(TextFileInputFormat.class);
		job.setOutputFormat(TextFileOutputFormat.class);
		job.setInputPath("CRT");
		job.setOutputPath("CRT_out");
		
		DataflowBuilder builder = new DataflowBuilder();
		VertexList v1 = builder.createVertexSet(TransformVertex.class, 2);
		VertexList v2 = builder.createVertexSet(CRTVertex.class, 2);
		VertexList v3 = builder.createVertexSet(CRTSumVertex.class, 1);
		
		builder.mapPointWise(v1, v2, ConnectorType.FILE);
		builder.crossProduct(v2, v3, ConnectorType.FILE);
		job.setRoot(v1);
		job.run();
	}
}
