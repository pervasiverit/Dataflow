# Dataflow
Write Data parallel programs using sequential building blocks.

### Writing a Vertex
Sequential Vertex is written by extending the AbstractVertex class and override the execute method.
```java
class MyVertex extends AbstractVertex<String>{
      @Override
      public void execute(String line, Collector collector){
      ....
      }
}
```

### Writing a Driver
```java
public class Main {

	public static void main(String [] args) throws BuilderException, IOException {
		DataFlowJob job = new DataFlowJob();
		job.setInputFormat(TextFileInputFormat.class);
		job.setInputPath("common.conf");
		DataflowBuilder builder = new DataflowBuilder();
		VertexList v1 = builder.createVertexSet(Hello.class, 2);
		VertexList v2 = builder.createVertexSet(Multiple.class, 2);
		VertexList v3 = builder.createVertexSet(Sum.class, 1);
		
		builder.mapPointWise(v1, v2, ConnectorType.FILE);
		builder.crossProduct(v2, v3, ConnectorType.FILE);
		job.start(v1);
		job.run();
	}
}
```
* In a driver program, we specify how we read the file using TextFileInputFormat.class

### Architecture
![Architecture](https://drive.google.com/open?id=0B2jG81b2klfnOW1helVyNktEczg)
