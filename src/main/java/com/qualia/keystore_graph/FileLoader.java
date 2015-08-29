package com.qualia.keystore_graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import com.google.common.io.CountingInputStream;

public abstract class FileLoader implements Runnable {

	protected final Status status;
	protected final File inputFile;
	protected boolean useGzip = true;
	private int linesToSend;
	private long bytesLastSent;
	
	public FileLoader(Status status, File inputFile) {
		this.status = status;
		this.inputFile = inputFile;
	}

	@Override
	public void run() {
		try {
			processFile();
		} catch (IOException e) {
			status.numErrors.incrementAndGet();
			System.out.println("Error loading file " + inputFile.getName() + "   " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	protected void processFile() throws IOException {
		FileInputStream fs = new FileInputStream(inputFile);
		CountingInputStream cs = new CountingInputStream(fs);
		InputStream nextStream = cs;
		if (useGzip) {
			nextStream = new GZIPInputStream(nextStream);
		}
		InputStreamReader isr = new InputStreamReader(nextStream);
		BufferedReader br = new BufferedReader(isr, 256 * 1024);

		String line;
		long curLine = 0;
		linesToSend = 0;
		bytesLastSent = 0;

		while ((line = br.readLine()) != null) {
			processLine(line, curLine);
			curLine++;
			linesToSend++;

			if (linesToSend >= 100) {
				sendStatus(cs);
			}
		}

		sendStatus(cs);
		br.close();
		status.numFiles.incrementAndGet();
	}

	private void sendStatus(CountingInputStream cs) {
		long bytesToSend = cs.getCount() - bytesLastSent;
		bytesLastSent = cs.getCount();
		status.numBytes.addAndGet(bytesToSend);
		status.numLines.addAndGet(linesToSend);
		linesToSend = 0;
	}

	public String getExtraStatus() {
		return "";
	}

	public abstract void processLine(String line, long curLine);
}
