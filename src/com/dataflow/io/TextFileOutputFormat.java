package com.dataflow.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.locks.ReentrantLock;

public class TextFileOutputFormat implements OutputFormat {

	private OutputStream stream;
	private PrintWriter writer;
	private File file;

	public TextFileOutputFormat(File file) {
		if (file == null) {
			throw new NullPointerException("File is null");
		}
		this.file = file;
	}

	@Override
	public void open() throws IOException {
		stream = new FileOutputStream(file.getAbsolutePath()+File.pathSeparator+"part");
		writer = new PrintWriter(stream);
	}

	@Override
	public void write(Object line) {
		writer.println(line);
	}

	@Override
	public void close() {
		writer.close();
		file = null;
	}

}
