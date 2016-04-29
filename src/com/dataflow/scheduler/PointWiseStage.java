package com.dataflow.scheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.dataflow.elements.Element;
import com.dataflow.io.Collector;
import com.dataflow.io.InputFormat;
import com.dataflow.io.IntermediateRecord;
import com.dataflow.vertex.AbstractVertex;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PointWiseStage extends Stage {

	private String tempPath = "tmp" + File.separator + job.getJobId() + File.separator + getTaskId();

	public PointWiseStage(DataFlowJob job) {
		super(job);
	}

	private static final long serialVersionUID = -6401487707823445353L;

	public <T> void run() throws IOException {
		if (job.getInputFormat() == null)
			throw new FileNotFoundException("Please provide file inputformat");

		InputFormat inf = job.getInputFormat();
		Collector<Element> collector = new Collector<>(tempPath);
		FileUtils.forceMkdir(new File(tempPath));
		if (inf != null) {
			inf.open();
			String line = "";
			AbstractVertex abs = queue.poll();
			while ((line = (String) inf.next()) != null) {
				
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
		String path = collector.finish();
		
	}

}
