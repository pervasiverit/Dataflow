import java.io.IOException;
import java.math.BigInteger;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.elements.TripleElement;
import com.dataflow.io.Collector;
import com.dataflow.utils.Triple;
import com.dataflow.vertex.AbstractVertex;

public class CRTVertex extends AbstractVertex<TripleElement>{

	@Override
	public void execute(TripleElement element, Collector collector) 
			throws IOException {
		Triple<BigInteger, BigInteger, BigInteger> triple = element.getElement();
		BigInteger B = triple.getThird().divide(triple.getSecond());
		BigInteger C = triple.getFirst();
		BigInteger X = ExtendedEuclidean.moduloInverse(B, triple.getSecond());
		
		BigIntegerElement product = new BigIntegerElement(B.multiply(C).multiply(X));
		collector.add(product);
	}

}
