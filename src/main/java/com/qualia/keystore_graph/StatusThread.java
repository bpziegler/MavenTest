package com.qualia.keystore_graph;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.time.DurationFormatUtils;

public class StatusThread extends Thread {

	private static final long STATUS_INTERVAL = 500;
	private static final int MB = 1024 * 1024;
	private static final Runtime runtime = Runtime.getRuntime();

	public AtomicBoolean done = new AtomicBoolean();

	private final Status status;
	private final long startTime;

	public StatusThread(Status status) {
		this.status = status;
		this.startTime = System.currentTimeMillis();
	}

	@Override
	public void run() {
		long lastLog = 0;

		while (!done.get()) {
			long now = System.currentTimeMillis();
			if (now - lastLog >= STATUS_INTERVAL) {
				lastLog = now;
				dumpStats(now);
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				break;
			}
		}
		
		dumpStats(System.currentTimeMillis());
	}

	private void dumpStats(long now) {
		long numBytes = status.numBytes.get();
		long totBytes = status.totBytes.get();
		long numLines = status.numLines.get();
		double elap = (0.0 + now - startTime) / 1000.0;
		double bytesPerSec = (elap > 0) ? numBytes / elap : 0;
		double linesPerSec = (elap > 0) ? numLines / elap : 0;
        double remainTime = (bytesPerSec > 0) ? (totBytes - numBytes) / bytesPerSec : 0;
        double percentDone = (totBytes > 0) ? (0.0 + numBytes) / totBytes : 0;
		long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / MB;
        String remainStr = DurationFormatUtils.formatDuration((long) remainTime * 1000, "H:mm:ss");
        String elapStr = DurationFormatUtils.formatDuration((long) elap * 1000, "H:mm:ss");
        
		String s = String.format("Elap %8s   Done %7.3f%%   Remain %8s   MB %,7d   Files %,5d   Err %,5d   Lines %,12d   Lines/Sec %,9.0f", 
				elapStr, percentDone * 100, remainStr, usedMB, status.numFiles.get(), status.numErrors.get(), numLines, linesPerSec);
		System.out.println(s);
	}

}
