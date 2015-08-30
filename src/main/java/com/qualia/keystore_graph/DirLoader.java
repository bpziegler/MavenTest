package com.qualia.keystore_graph;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.joda.time.DateTime;

public class DirLoader {
	
	private final List<File> files = new ArrayList<File>();
	
	public void loadAllFiles(File loadDir) throws InterruptedException {
		System.out.println("Started at " + DateTime.now());
		System.out.println("Scanning Files Dir = " + loadDir.getAbsolutePath());
		recurPopulateFiles(loadDir);
		
		long totBytes = 0;
		
		for (File oneFile : files) {
			totBytes += oneFile.length();
		}
		System.out.println(String.format("Files %,d   Bytes %,d", files.size(), totBytes));
		
		Status status = new Status();
		status.totBytes.set(totBytes);
		status.totFiles.set(files.size());
		StatusThread statusThread = new StatusThread(status);
		statusThread.start();
		
		int numCpu = Runtime.getRuntime().availableProcessors();
		numCpu = Math.max(2, numCpu-2);
		ExecutorService service = Executors.newFixedThreadPool(numCpu);
		
		// Process all files
		for (File oneFile : files) {
			String path = oneFile.getAbsolutePath();
			String pathLower = path.toLowerCase();
			String base = loadDir.getAbsolutePath();
			String relative = new File(base).toURI().relativize(new File(path).toURI()).getPath();
			
			if (pathLower.contains("cookie")) {
				CookieLoader cookieLoader = new CookieLoader(status, oneFile, relative);
				service.submit(cookieLoader);
			} else if (pathLower.contains("addthis")) {
				throw new RuntimeException("test");
			} else {
				System.out.println("Can't autodetect file " + relative);
			}
		}
		
		service.shutdown();
		service.awaitTermination(100, TimeUnit.DAYS);
		
		System.out.println("Closing database at " + DateTime.now());
		Database.close();
		
		statusThread.done.set(true);
		statusThread.join();

		System.out.println("Finished at " + DateTime.now());
	}
	
	private void recurPopulateFiles(File dir) {
		File[] curFiles = dir.listFiles();
		for (File oneFile : curFiles) {
			String ext = FilenameUtils.getExtension(oneFile.getName());
			if (ext.equals("done")) continue;		// Skip over .done files
			if (ext.equals("rb")) continue;			// Skip over .rb files
			if (oneFile.isDirectory()) {
				recurPopulateFiles(oneFile);
			} else {
				files.add(oneFile);
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		String dir = "test_data";
		if (args.length > 0) dir = args[0];

		DirLoader dirLoader = new DirLoader();
		dirLoader.loadAllFiles(new File(dir));
	}

}
