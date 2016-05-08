package com.dataflow.partitioner;

import java.io.IOException;
import java.io.Serializable;

@FunctionalInterface
public interface Partitioner extends Serializable{
	public void partition(String filePath, int partitionCount) 
			throws IOException;
}
