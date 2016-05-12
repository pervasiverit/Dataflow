import java.io.IOException;
import java.math.BigInteger;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.io.Collector;
import com.dataflow.vertex.AbstractVertex;

public class Sum extends AbstractVertex<BigIntegerElement>{
	private BigInteger total = BigInteger.ZERO;
	
	@Override
	public void execute(BigIntegerElement line, Collector collector) throws IOException {
		total = total.add(line.getElement());
		System.out.println(total);
	}
	
	@Override
	public void close(Collector collector){
		try {
			collector.add(new BigIntegerElement(total));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
