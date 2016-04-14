import java.io.IOException;

import com.dataflow.utils.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Hello extends AbstractVertex<String>{
	
	private static final long serialVersionUID = -1749891852729638953L;

	@Override
	public void execute(String line, Collector collector) throws IOException {
		System.out.println(line.length());
		collector.add(line.length());
	}
}
