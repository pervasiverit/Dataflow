import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.dataflow.elements.TripleElement;
import com.dataflow.io.Collector;
import com.dataflow.utils.Triple;
import com.dataflow.vertex.AbstractVertex;

public class TransformVertex extends AbstractVertex<String>{

	private long multiplied = 0;
	
	@Override
	public void start(Collector collector) throws Exception{
		
		multiplied = Files.lines(Paths.get("CRT"))
			 .mapToLong(e -> Long.parseLong(e.split(",")[1]))
			 .reduce(1L, (c,e) -> c * e);
	}
	
	@Override
	public void execute(String line, Collector collector) throws IOException {
		String arr[] = line.split(",");
		BigInteger ele1 = new BigInteger(arr[0]);
		BigInteger ele2 = new BigInteger(arr[1]);
		Triple<BigInteger, BigInteger, BigInteger> tripleElement = 
				new Triple<>(ele1, ele2, BigInteger.valueOf(multiplied));
		collector.add(new TripleElement(tripleElement));
	}
	
	public static void main(String[] args) throws Exception {
		TransformVertex vertex = new TransformVertex();
		Collector collector = new Collector<>("path");
		vertex.start(collector);
		System.out.println(vertex.multiplied);
		vertex.execute("3,4", collector);
		vertex.execute("1,7", collector);
		vertex.execute("2,5", collector);
		System.out.println(collector.getBuffer());
	}

}
