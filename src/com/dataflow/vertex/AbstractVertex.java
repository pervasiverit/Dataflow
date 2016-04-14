package com.dataflow.vertex;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.util.UUID;

import org.apache.commons.lang3.SerializationUtils;

import com.dataflow.edges.Edge;
import com.dataflow.edges.EdgeList;
import com.dataflow.utils.Collector;
import com.dataflow.utils.ConnectorType;

public abstract class AbstractVertex<T> implements Serializable {

	private static final long serialVersionUID = 7167879471388118185L;

	private static int COUNT= 0;
	
	protected String vertexId;

	protected transient String name;

	protected EdgeList inputEdges;
	protected EdgeList outputEdges;

	public AbstractVertex() {
		
		this.inputEdges = new EdgeList();
		this.outputEdges = new EdgeList();
		vertexId = "Abstract Vertex " + this.getClass().toString()+ " "+this.hashCode();
		machine = "glados.cs.rit.edu";
		
	}

	private VertexInfo vertexInfo;
	private String machine;

	public String getVertexId() {
		return vertexId;
	}

	public EdgeList getInput() {
		return inputEdges;
	}

	public EdgeList getOutput() {
		return outputEdges;
	}

	public abstract void execute(T Line, Collector collector) throws IOException;

	public void connectOutput(final int remotePort, AbstractVertex outputVertex, final int localPort,
			ConnectorType type) {

		Edge edge = new Edge(outputVertex, remotePort, type);
		outputEdges.set(localPort, edge);
		edge = new Edge(this, localPort, type);
		outputVertex.getInput().set(remotePort, edge);
	}

	@Override
	public String toString() {
		return vertexId;
	}

	// Dependency on the commons-lang library.
	public AbstractVertex makeClone()  {
		byte [] b = SerializationUtils.serialize(this);
		AbstractVertex vertex = SerializationUtils.deserialize(b);
		return vertex;
	}

}
