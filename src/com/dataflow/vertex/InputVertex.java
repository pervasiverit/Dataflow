package com.dataflow.vertex;

import java.io.IOException;

import com.dataflow.io.InputFormat;
import com.dataflow.utils.Collector;

public class InputVertex extends AbstractVertex{

	private static final long serialVersionUID = 7171383548464323992L;
	private static InputFormat<?> inputFormat;
	
	@Override
	public String toString(){
		return getVertexId();
	}

	public InputVertex(InputFormat<?> inputFormat){
		InputVertex.inputFormat = inputFormat;
	}
	
	@Override
	public void execute(final String Line, final Collector collector) throws IOException {
		inputFormat.open();
		String str= "";
		while((str = (String) inputFormat.next())!=null){
			System.out.println(str);
			collector.add(str);
		}
		inputFormat.close();
	}
	
}
