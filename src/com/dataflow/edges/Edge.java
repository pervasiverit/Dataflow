package com.dataflow.edges;

import com.dataflow.utils.ConnectorType;
import com.dataflow.vertex.AbstractVertex;

public class Edge {

	private AbstractVertex remoteVertex;
	private final int port;
	private ConnectorType type;
	
	public Edge(){
		port = 0;
	}
	
	public Edge(AbstractVertex rVertex, final int port, ConnectorType type){
		this.port = port;
		this.remoteVertex = rVertex;
		this.type = type;
	}
	
	public AbstractVertex getRemoteVertex(){
		return remoteVertex;
	}
	
	public int getPort(){
		return port;
	}
	
	public ConnectorType getConnectorType(){
		return type;
	}
	
	
	@Override
	public String toString(){
		return "An Edge with remote vertex: "+remoteVertex;
	}
	
}
