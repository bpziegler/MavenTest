package com.localresponse.tapad;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;
import com.localresponse.util.CountMap;


public class FileStats {

	private static final int SLOW_STATS_INTERVAL_SECONDS = 10;

	private long lasCalcStatTime;
	private Splitter tabSplitter = Splitter.on("\t");
	private Splitter equalSplitter = Splitter.on("=");

	private CountMap idTypes = new CountMap();
	private CountMap platforms = new CountMap();


	private void run(String tapadFile) throws IOException {
		List<File> list = new ArrayList<File>();
		list.add(new File(tapadFile));
		MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
		multiLineProcessor.processFiles(list, new ILineProcessor() {
			public void processLine(String line, long curLine) {
				FileStats.this.processLine(line, curLine);
			}


			public String getStatus() {
				if (System.currentTimeMillis() - lasCalcStatTime >= SLOW_STATS_INTERVAL_SECONDS * 1000) {
					lasCalcStatTime = System.currentTimeMillis();
					calcSlowStats();
				}

				String status = FileStats.this.getStatus();

				return status;
			}

		});

		calcSlowStats();
		System.out.println(getStatus());
	}


	protected void calcSlowStats() {
		try {
			Files.write(idTypes.report(), new File("idtypes-stats.txt"), Charsets.UTF_8);
			Files.write(platforms.report(), new File("platforms-stats.txt"), Charsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	protected String getStatus() {
		return String.format("idTypes %2d   platforms %3d", idTypes.size(), platforms.size());
	}


	protected void processLine(String line, long curLine) {
		List<String> devices = tabSplitter.splitToList(line);
		for (String oneDevice : devices) {
			List<String> parts = equalSplitter.splitToList(oneDevice);
			String idTypeStr = parts.get(0);
			String platformStr = parts.get(2);
			idTypes.updateCount(idTypeStr);
			platforms.updateCount(platformStr);
		}
	}


	public static void main(String[] args) throws Exception {
		String tapadFile = "/Users/benziegler/work/tapad/LocalResponse_ids_full_20140827_203357";

		if (args.length > 0) {
			tapadFile = args[0];
			System.out.println("tapadFile = " + tapadFile);
		}

		FileStats fileStats = new FileStats();

		fileStats.run(tapadFile);
	}
}
