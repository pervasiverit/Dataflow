import java.io.IOException;

import com.dataflow.utils.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Hello extends AbstractVertex{

	@Override
	public void execute(String line, Collector collector) throws IOException {
		System.out.println(line.chars().count());
	}


}
