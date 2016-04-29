package com.dataflow.vertex;

import java.io.IOException;

import com.dataflow.elements.StringElement;
import com.dataflow.io.Collector;
import com.dataflow.io.InputFormat;

@SuppressWarnings({"rawtypes","unchecked"})
public class InputVertex extends AbstractVertex<String>{

	private static final long serialVersionUID = 7171383548464323992L;
	private static InputFormat<?> inputFormat;
	
	@Override
	public String toString(){
		return "IO Vertex" + " " + this.hashCode();
	}

	public InputVertex(InputFormat<?> inputFormat){
		InputVertex.inputFormat = inputFormat;
	}
	
	@Override
	public void execute(final String Line, final Collector collector) throws IOException {
		
		inputFormat.open();
		String str= "";
		while((str = (String) inputFormat.next())!=null){
			collector.add(new StringElement(str));
		}
		inputFormat.close();
	}
	
}
