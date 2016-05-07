package com.dataflow.scheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.dataflow.elements.Element;
import com.dataflow.io.Collector;
import com.dataflow.io.InputFormat;
import com.dataflow.io.IntermediateRecord;
import com.dataflow.io.OutputFormat;
import com.dataflow.vertex.AbstractVertex;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PointWiseStage extends Stage {

	private String tempPath = "tmp" + File.separator + getJobId() + File.separator + getTaskId();
	private File file;
	final protected Class<? extends InputFormat> inputFormat;
	private static int stageIncr;
	private final static int stageID;
	static {
		stageID = stageIncr;
		stageIncr++;
	}
	public PointWiseStage(Class<? extends InputFormat> inputFormat, String jobId, File file) {
		super(jobId);
		this.file = file;
		this.inputFormat = inputFormat;
	}

	private static final long serialVersionUID = -6401487707823445353L;

	public <T> void run() throws IOException {
		if (getInputFormat() == null)
			throw new FileNotFoundException("Please provide file inputformat");
		InputFormat infInstance = createInstance();
		Collector<Element> collector = new Collector<>(tempPath);
		
		FileUtils.forceMkdir(new File(tempPath));
		if (infInstance != null) {
			infInstance.open();
			String line = "";
			AbstractVertex abs = queue.poll();
			while ((line = (String) infInstance.next()) != null) {
				abs.execute(line, collector);
			}
		}
		final int size = queue.size();

		AbstractVertex mapSide = null;
		for (int i = 0; i < size; ++i) {
			mapSide = queue.poll();
			List<IntermediateRecord<Element>> buffer = collector.getBuffer();
			collector.clearBuffer();
			for(IntermediateRecord<Element> element: buffer){
				mapSide.execute(element.getElement(), collector);
			}
			
		}
		collector.finish();
		
	}
	
	private Class<? extends InputFormat> getInputFormat(){
		return inputFormat;
	}
	
	
	private InputFormat createInstance() {
		Constructor<? extends InputFormat> cons;
		InputFormat inf = null;
		long length = file.length();
		long temp = (long)Math.ceil((length/ stageIncr));
		long offset = stageID * temp;
		long end = offset + temp;
		try {
			cons = inputFormat.getConstructor(File.class);
		} catch (NoSuchMethodException | SecurityException err) {
			throw new RuntimeException("Error while creating an input constructor");
		}
		try {
			inf = (InputFormat) cons.newInstance(file, offset,end);
		} catch (Exception e) {

		}
		return inf;
	}


	@Override
	public String toString() {
		return tempPath;
	}


	public String getPath() {
		return tempPath;
	}

}
