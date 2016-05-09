package com.dataflow.io;

import java.io.File;
import com.google.code.externalsorting.ExternalSort;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.dataflow.elements.Element;

public class Collector<T extends Element> implements Iterable<IntermediateRecord<T>>{

	private final List<IntermediateRecord<T>> buffer;
	private final String tmpDir;
	/*private static AtomicInteger tmpFileSeq = new AtomicInteger(0);
	private List<String> splitPaths = new ArrayList<>();

	private final static int bufferSize = 10000;*/

	public Collector(String path) {
		buffer = new LinkedList<>();
		this.tmpDir = path;
	}
	
	public void clearBuffer(){
		buffer.clear();
	}
	
	/*private String getFilePath() {
		return this.tmpDir + File.separator + "split_" + tmpFileSeq.get();
	}*/

	public void add(T element) throws IOException {
		buffer.add(new IntermediateRecord<T>(element));
		/*System.out.println(element);
		if (buffer.size() >= bufferSize) {
			snapshot();
		}*/
	}

	public String finish() throws IOException {
		String path = this.tmpDir + File.separator + "records.sorted " + Thread.currentThread().getId();
		snapshot(path);
		//sortSplitFiles(path);
		return path;
	}
	
	public List<IntermediateRecord<T>> getBuffer(){
		return Collections.unmodifiableList(new LinkedList<>(buffer));
	}

	/*private void sortSplitFiles(String path) throws IOException {
		List<File> files = splitPaths
							.stream()
							.map(e -> new File(e))
							.collect(Collectors.toList());
		try {
			ExternalSort.mergeSortedFiles(files,new File(path));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}*/

	private void snapshot(String path) throws IOException {
		Collections.sort(buffer);
		//String path = getFilePath();
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
		for (IntermediateRecord<T> record : buffer) {
			out.writeObject(record.getElement());
		}
		out.writeObject(null);
		out.flush();
		out.close();
		buffer.clear();
		/*splitPaths.add(path);
		tmpFileSeq.getAndIncrement();*/
	}

	// TODO: Change this..
	@Override
	public String toString() {
		return "Collector" + " ";
	}

	@Override
	public Iterator<IntermediateRecord<T>> iterator() {
		return buffer.iterator();
	}

}
