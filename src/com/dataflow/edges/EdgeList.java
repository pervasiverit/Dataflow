package com.dataflow.edges;

import java.util.ArrayList;

public class EdgeList extends ArrayList<Edge>{

	public void addEdges(final int newInputs) {
		for(int i=0; i< newInputs; i++){
			add(new Edge());
		}
	}

	public void addNumberOfEdges(final int nbrDest) {
		for(int i=0; i< nbrDest; i++){
			add(new Edge());
		}
	}
	
	
}
