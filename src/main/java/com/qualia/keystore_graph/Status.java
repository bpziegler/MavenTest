package com.qualia.keystore_graph;

import java.util.concurrent.atomic.AtomicLong;

public class Status {

	public final AtomicLong numFiles = new AtomicLong();
	public final AtomicLong numBytes = new AtomicLong();
	public final AtomicLong numLines = new AtomicLong();
	public final AtomicLong totFiles = new AtomicLong();
	public final AtomicLong totBytes = new AtomicLong();
	public final AtomicLong numErrors = new AtomicLong();
	
}
