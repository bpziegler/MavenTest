package com.localresponse.add_this_mapping;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class MemoryCompact implements ILineProcessor {

	private final MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
	private final String dir;
	private final Splitter tabSplitter = Splitter.on("\t");
	private final Splitter commaSplitter = Splitter.on(",");
	private final Splitter equalSplitter = Splitter.on("=");
	
	private final TLongSet addThisSet = new TLongHashSet();
	private final TLongSet appNexusSet = new TLongHashSet();
	private final HashFunction hashFunction = Hashing.murmur3_128();
	private BufferedWriter bw;


	public MemoryCompact(String dir) {
		this.dir = dir;
	}


	private void compact() throws IOException {
		File fileDir = new File(dir);
		List<File> files = Arrays.asList(fileDir.listFiles());
		
		FileOutputStream fs = new FileOutputStream("addthis_reorder.txt");
		OutputStreamWriter osw = new OutputStreamWriter(fs);
		bw = new BufferedWriter(osw, 256 * 1024);
		
		lineProcessor.processFiles(files, this);
		
		bw.close();
	}


	public void processLine(String line, long curLine) {
		String[] ary = Iterables.toArray(String.class, tabSplitter.split(line));
		String timestamp = ary[0];
		String lr = ary[1];
		String mappedCookies = ary[2];
		
		String appNexus = null;
		String addThis = null;
				
		String[] cookies = Iterables.toArray(String.class, equalSplitter.split(mappedCookies));
		for (String oneCookie : cookies) {
			String[] parts = Iterables.toArray(String.class, equalSplitter.split(oneCookie));
			if (parts.length != 2) continue;
			if (parts[0].equals("6")) {
				appNexus = parts[1];
			} else if (parts[0].equals("9")) {
				addThis = parts[1];
			}
		}
		
		if (isInvalid(appNexus) || isInvalid(addThis)) return;
		
		String newLine = String.format("%s\t%s\t%s\n", addThis, appNexus, timestamp);
		try {
			bw.write(newLine);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	private static boolean isInvalid(String cookie) {
		return "0".equals(cookie) || "-1".equals(cookie);
	}


	private void processAddThisCookie(String cookie) {
		long hashVal = hashFunction.hashString(cookie, Charsets.UTF_8).asLong();
		addThisSet.add(hashVal);
	}


	private void processAppNexusCookie(String cookie) {
		long hashVal = hashFunction.hashString(cookie, Charsets.UTF_8).asLong();
		appNexusSet.add(hashVal);
	}


	public static void main(String[] args) throws Exception {
		String dir = args[0];
		MemoryCompact memoryCompact = new MemoryCompact(dir);
		memoryCompact.compact();
	}


	public String getStatus() {
		return null;
	}

}
