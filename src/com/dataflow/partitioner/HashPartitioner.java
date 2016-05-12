package com.dataflow.partitioner;

import com.dataflow.elements.Element;

public class HashPartitioner implements Partitioner<Integer>{

	private static final long serialVersionUID = 1L;

	@Override
	public Integer partitionLogic(Element<?> element, int partitionCount) {
		return element.hashCode() % partitionCount;
	}
		
}
