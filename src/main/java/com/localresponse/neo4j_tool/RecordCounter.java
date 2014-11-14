package com.localresponse.neo4j_tool;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class RecordCounter {

	private String filename;
	private int recordLen = 9;

	public RecordCounter(String filename) {
		this.filename = filename;
	}

	private void run() throws IOException {
		File inputFile = new File(filename);
		long size = inputFile.length();
		
		if (size % recordLen != 0) {
			throw new RuntimeException("Filename is not an even multiple of RecordLen " + recordLen);
		}
		
		long numRec = size / recordLen;
		System.out.println("numRec = " + numRec);
		
		FileInputStream fs = new FileInputStream(filename);
		BufferedInputStream bs = new BufferedInputStream(fs);
		DataInputStream ds = new DataInputStream(bs);
		
		byte[] buf = new byte[recordLen];
		
		for (long i = 0; i < numRec; i++) {
			ds.readFully(buf, 0, recordLen);
		}
		
		ds.close();
	}

	public static void main(String[] args) throws Exception {
		String filename = "/Users/benziegler/work/neo4j-community-2.1.2/data/graph.db/neostore.nodestore.db";
		if (args.length > 0) {
			filename = args[0];
		}
		
		RecordCounter recordCounter = new RecordCounter(filename);
		recordCounter.run();
	}

}
