import java.io.IOException;
import java.math.BigInteger;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.io.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Hello extends AbstractVertex<String>{
	
	private static final long serialVersionUID = -1749891852729638953L;

	@Override
	public void execute(String line, Collector collector) throws IOException {
		collector.add(new BigIntegerElement(BigInteger.valueOf(line.length())));
	}
}
