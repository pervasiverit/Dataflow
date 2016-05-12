package com.dataflow.partitioner;

import com.dataflow.elements.Element;

public class HashPartitioner implements Partitioner{

	private static final long serialVersionUID = 1L;

	@Override
	public Integer partitionLogic(Element<?> element, int partitionCount) {
		System.out.println(element +" :"+ (element.hashCode() & Integer.MAX_VALUE) % partitionCount);
		return (element.hashCode() & Integer.MAX_VALUE) % partitionCount;
	}
		
}
