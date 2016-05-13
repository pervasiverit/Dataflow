package com.dataflow.partitioner;

import java.io.Serializable;

import com.dataflow.elements.Element;

/**
 * Partitioner interface - Implement it to specify the required 
 * partitioning logic that will be used will executing PointwWise- 
 * Stage(Map) to write the partition files.
 * 
 * @author KanthKumar
 *
 */
@FunctionalInterface
public interface Partitioner extends Serializable{
	/**
	 * Specify the logic that determine which partition this element
	 * will goto
	 * 
	 * @param element	
	 * @param partitionCount
	 * @return Partition index(Partition file number)
	 */
	public Integer partitionLogic(Element<?> element, int partitionCount);
}
