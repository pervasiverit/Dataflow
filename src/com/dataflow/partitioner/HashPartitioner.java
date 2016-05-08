package com.dataflow.partitioner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HashPartitioner implements Partitioner{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void partition(String filePath, int partitionCount) 
			throws IOException {
		Path path = Paths.get(filePath);
		String partitionPath = path.getParent().toString() + File.separator 
				+ "partition";
		FileInputStream fis = new FileInputStream (path.toFile());
		ObjectInputStream stream = new ObjectInputStream(fis);
		Object obj;
		try {
			while((obj = stream.readObject()) != null) { 
				FileOutputStream fos = new FileOutputStream(new 
						File(partitionPath + obj.hashCode() % partitionCount));
				ObjectOutputStream output = new ObjectOutputStream(fos);
				output.writeObject(obj);
				output.close();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
