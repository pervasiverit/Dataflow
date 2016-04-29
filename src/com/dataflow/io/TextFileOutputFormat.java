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
	private final ReentrantLock lock = new ReentrantLock();

	public TextFileOutputFormat(File file) {
		if (file == null) {
			throw new NullPointerException("File is null");
		}
		this.file = file;
	}

	@Override
	public void open() throws IOException {
		lock.lock();
		try {
			stream = new FileOutputStream(file);
			writer = new PrintWriter(stream);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void write(Object line) {
		writer.println(line);
	}

	@Override
	public void close() {
		lock.lock();
		try {
			writer.close();
			file = null;
		} finally {
			lock.unlock();
		}
	}

}
