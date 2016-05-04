package com.dataflow.scheduler;

import java.io.IOException;

import com.dataflow.io.InputFormat;

public class CrossProductStage extends Stage{

	public CrossProductStage(InputFormat inputFormat, String jobId) {
		super(inputFormat, jobId);
	}

	private static final long serialVersionUID = 7387991910464657727L;

	@Override
	public <T> void run() throws IOException {
		
	}

}
