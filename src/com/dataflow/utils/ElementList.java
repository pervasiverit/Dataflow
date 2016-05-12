package com.dataflow.utils;

import java.util.ArrayList;
import java.util.List;

import com.dataflow.elements.Element;

public class ElementList extends ArrayList<Element>{
	
	public void addElements(List<Element> element){
		addAll(element);
	}

}
