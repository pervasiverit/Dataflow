package com.dataflow.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Text File Input Format.
 * 
 * @author sumanbharadwaj
 *
 */
public class TextFileInputFormat implements InputFormat<String> {

	private File file;
	private BufferedReader stream;
	private FileReader fis;
	// for collecting stats.
	private long numRecords;

	private final ReentrantLock lock = new ReentrantLock();

	public TextFileInputFormat(File file) {
		if (file == null) {
			throw new NullPointerException("File information is < null >");
		}
		this.file = file;
	}

	@Override
	public void open() throws IOException {
		lock.lock();
		try {
			if (fis == null) {
				fis = new FileReader(file);
				stream = new BufferedReader(fis);
				numRecords = 0;
			}
		} finally {
			lock.unlock();
		}

	}

	@Override
	public String next() throws IOException {
		lock.lock();
		try {
			if (stream == null) {
				throw new IOException("Unable to Open the file");
			}
			String line = stream.readLine();
			++numRecords;
			return line != null ? line : null;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void close() throws IOException {
		lock.lock();
		try {
			if (stream == null) {
				throw new IOException("Unable to close the file");
			}
			fis = null;
			stream = null;

		} finally {
			lock.unlock();
		}

	}

	@Override
	public String toString() {
		return "Total number of records is " + numRecords;
	}

	/**
	 * Unit Test. To test threading.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		File file = new File("common.conf");
		TextFileInputFormat text = new TextFileInputFormat(file);

		class pt extends Thread {

			TextFileInputFormat text;

			public pt(TextFileInputFormat text) throws IOException {
				this.text = text;
				text.open();
			}

			@Override
			public void run() {
				try {
					String str;
					while ((str = text.next()) != null) {
						System.out.println(this + " " + str);
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

		pt p = new pt(text);
		pt p1 = new pt(text);

		p.start();
		p1.start();
	}

}
