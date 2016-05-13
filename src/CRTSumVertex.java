import java.io.IOException;
import java.math.BigInteger;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.elements.TripleElement;
import com.dataflow.io.Collector;
import com.dataflow.utils.Triple;
import com.dataflow.vertex.AbstractVertex;

public class CRTSumVertex extends AbstractVertex<TripleElement>{
	BigInteger CRT = BigInteger.ZERO;
	BigInteger B = BigInteger.ZERO;
	
	@Override
	public void execute(TripleElement element, Collector collector)
			throws IOException {
		Triple<BigInteger, BigInteger, BigInteger> triple = element.getElement();
		CRT = CRT.add(triple.getFirst());
		B = triple.getSecond();
	}

	@Override
	public void close(Collector collector) {
		System.out.println("Chinese reminder theorem value : "+CRT.remainder(B));
		try {
			collector.add(new BigIntegerElement(CRT));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
