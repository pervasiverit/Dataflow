package com.dataflow.partitioner;

import java.io.Serializable;

import com.dataflow.elements.Element;

@FunctionalInterface
public interface Partitioner<T> extends Serializable{
	public T partitionLogic(Element<?> element, int partitionCount);
}
