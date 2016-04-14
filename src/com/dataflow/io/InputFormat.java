package com.dataflow.io;

import java.io.IOException;

public interface InputFormat<T>{
	public void open() throws IOException;
	public T next() throws IOException;
	public void close() throws IOException;
}
