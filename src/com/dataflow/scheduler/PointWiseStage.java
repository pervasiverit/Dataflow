package com.dataflow.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import com.dataflow.elements.Element;
import com.dataflow.io.Collector;
import com.dataflow.io.InputFormat;
import com.dataflow.io.IntermediateRecord;
import com.dataflow.partitioner.HashPartitioner;
import com.dataflow.partitioner.Partitioner;
import com.dataflow.vertex.AbstractVertex;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PointWiseStage extends Stage {

	final private String tempPath = "tmp" + File.separator + getJobId() + File.separator + getTaskId();
	final private File file;
	final protected Class<? extends InputFormat> inputFormat;
	final private Class<? extends Partitioner> partitioner;

	public PointWiseStage(Class<? extends InputFormat> inputFormat, Class<? extends Partitioner> partitioner,
			String jobId, File file) {
		super(jobId);
		this.file = file;
		this.inputFormat = inputFormat;
		this.partitioner = partitioner;
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
			for (IntermediateRecord<Element> element : buffer) {
				mapSide.execute(element.getElement(), collector);
			}

		}

		String collectedFile = collector.finish();
		Optional<Partitioner> ptnr = createPartitionerInstance();
		if (ptnr.isPresent()) {
			partition(collectedFile, ptnr.get());
		}
	}

	private Class<? extends InputFormat> getInputFormat() {
		return inputFormat;
	}

	private InputFormat createInstance() {
		Constructor<? extends InputFormat> cons;
		InputFormat inf = null;
		long length = file.length();
		long temp = (long) Math.ceil((length / stageIncr));
		long offset = stageID * temp;
		long end = offset + temp;
		System.out.println(String.format("StageID %d TotalStages %d", stageID, stageIncr));
		try {
			cons = inputFormat.getConstructor(File.class, Long.class, Long.class);
		} catch (NoSuchMethodException | SecurityException err) {
			throw new RuntimeException("Error while creating an input constructor");
		}
		try {
			inf = (InputFormat) cons.newInstance(file, offset, end);
		} catch (Exception e) {

		}
		return inf;
	}

	private Optional<Partitioner> createPartitionerInstance() {
		Optional<Partitioner> optional;
		Partitioner ptnr = null;
		try {
			ptnr = partitioner.newInstance();
		} catch (Exception e) {
			System.err.println("Error creating partitioner instance, using "
					+ "default Hash Partitioner");
			ptnr = new HashPartitioner();
		}
		optional = Optional.ofNullable(ptnr);
		return optional;
	}

	@Override
	public String toString() {
		return tempPath;
	}

	public String getPath() {
		return tempPath;
	}

	private void partition(String filePath, Partitioner ptnr) throws IOException {
		Path path = Paths.get(filePath);
		String partitionPath = path.getParent().toString() + File.separator
				+ "partition_";
		FileInputStream fis = new FileInputStream (path.toFile());
		
		List<ObjectOutputStream> partitionOuts = new ArrayList<>();
		Map<Integer, String> partitionFiles = new HashMap<>();
		
		ObjectOutputStream output;
		for(int i=0; i<partitionCount; i++){
			partitionFiles.put(i, partitionPath + i);
			output = new ObjectOutputStream(new FileOutputStream
					(new File(partitionPath + i)));
			partitionOuts.add(output);
		}
		
		try(ObjectInputStream stream = new ObjectInputStream(fis)) {
			Element ele;
			while((ele = (Element)stream.readObject()) != null) {
				output = partitionOuts	
							.get(ptnr.partitionLogic(ele, partitionCount));
				output.writeObject(ele.getElement());
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		setPartitionFiles(partitionFiles);
		for(ObjectOutputStream out : partitionOuts){
			out.writeObject(null);
			out.close();
		}
	}
	
}


