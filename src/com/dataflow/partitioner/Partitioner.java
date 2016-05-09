package com.dataflow.partitioner;

import java.io.Serializable;

import com.dataflow.elements.Element;

@FunctionalInterface
public interface Partitioner extends Serializable{
	public Integer partitionLogic(Element<?> element, int partitionCount);
}
