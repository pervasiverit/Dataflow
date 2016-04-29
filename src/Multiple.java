import java.io.IOException;
import java.math.BigInteger;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.io.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Multiple extends AbstractVertex<BigIntegerElement>{

	private static final long serialVersionUID = 8983135386205518370L;

	@Override
	public void execute(BigIntegerElement line, Collector collector) throws IOException {
		System.out.println(line.getElement().multiply(BigInteger.valueOf(2))+ Thread.currentThread().getName());
		collector.add(new BigIntegerElement(line.getElement().multiply(BigInteger.valueOf(2))));
	}
	
}
