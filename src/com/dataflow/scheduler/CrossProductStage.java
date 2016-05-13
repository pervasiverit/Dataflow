package com.dataflow.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.dataflow.elements.BigIntegerElement;
import com.dataflow.elements.Element;
import com.dataflow.io.Collector;
import com.dataflow.io.IntermediateRecord;
import com.dataflow.io.OutputFormat;
import com.dataflow.utils.ElementList;
import com.dataflow.vertex.AbstractVertex;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CrossProductStage extends Stage {

	final private String tempPath = "tmp" + File.separator + getJobId() + File.separator + getTaskId();
	private final Class<? extends OutputFormat> outputFormat;
	private final File file;

	private File tempFile;

	private ElementList elementList;

	public CrossProductStage(final Class<? extends OutputFormat> outputFormat, final String jobId, final File outFile) {
		super(jobId);
		this.outputFormat = outputFormat;
		this.file = outFile;
	}

	private static final long serialVersionUID = 7387991910464657727L;

	@Override
	public void run() throws IOException {
		
		Collector<Element> collector = new Collector<>(tempPath);
		AbstractVertex abv = queue.poll();

		for(Element element : elementList){
			abv.execute(element, collector);
		}
		abv.close(collector);
		final int size = queue.size();
		FileUtils.forceMkdir(file);
		
		AbstractVertex reduceSide = null;
		for (int i = 0; i < size; ++i) {
			reduceSide = queue.poll();
			List<IntermediateRecord<Element>> buffer = collector.getBuffer();
			collector.clearBuffer();
			for(IntermediateRecord<Element> element: buffer){
				reduceSide.execute(element.getElement(), collector);
			}
		}
		OutputFormat format = createInstance();
		if(format !=null){
			format.open();
			for(Object e : collector){
				System.out.println(((BigIntegerElement)e).getElement());
				format.write(e);
			}
		}
		format.close();
	}
	
	
	public void setElementList(ElementList e) {
		this.elementList = e;
	}

	private OutputFormat createInstance() {
		Constructor<? extends OutputFormat> cons;
		OutputFormat inf = null;
		try {
			cons = outputFormat.getConstructor(File.class);
		} catch (NoSuchMethodException | SecurityException err) {
			throw new RuntimeException("Error while creating an input constructor");
		}
		try {
			inf = (OutputFormat) cons.newInstance(file);
		} catch (Exception e) {

		}
		return inf;
	}

}
