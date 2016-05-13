import java.io.IOException;
import java.math.BigInteger;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.io.Collector;
import com.dataflow.vertex.AbstractVertex;

public class CRTSumVertex extends AbstractVertex<BigIntegerElement>{
	BigInteger CRT = BigInteger.ZERO;
	
	@Override
	public void execute(BigIntegerElement element, Collector collector)
			throws IOException {
		CRT.add(element.getElement());
	}

	@Override
	public void close(Collector collector) {
		System.out.println("Chinese reminder theorem value : "+CRT);
		try {
			collector.add(new BigIntegerElement(CRT));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
