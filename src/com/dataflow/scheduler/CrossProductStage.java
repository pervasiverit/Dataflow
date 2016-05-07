package com.dataflow.scheduler;

import java.io.File;
import java.io.IOException;

import com.dataflow.io.OutputFormat;

public class CrossProductStage extends Stage {

	private final Class<? extends OutputFormat> outputFormat;
	private final File file;

	public CrossProductStage(final Class<? extends OutputFormat> outputFormat, 
							 final String jobId, 
							 final File outFile) {
		super(jobId);
		this.outputFormat = outputFormat;
		this.file = outFile;
	}

	private static final long serialVersionUID = 7387991910464657727L;

	@Override
	public <T> void run() throws IOException {

	}

}
