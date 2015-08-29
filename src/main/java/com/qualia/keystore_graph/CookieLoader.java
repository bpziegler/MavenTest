package com.qualia.keystore_graph;

import java.io.File;
import java.io.IOException;

public class CookieLoader extends FileLoader {

	private final GraphStorage graphStorage;

	public CookieLoader(Status status, File inputFile) {
		super(status, inputFile);
		graphStorage = new GraphStorage(false);
	}

	@Override
	public void processFile() throws IOException {
		super.processFile();
		graphStorage.close();
	}

	@Override
	public void processLine(String line, long curLine) {
	}

}
