package com.qualia.keystore_graph;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.time.DurationFormatUtils;

import com.google.common.io.CountingInputStream;
import com.qualia.util.ILineProcessor;


public abstract class FileLoader {

    private final int MB = 1024 * 1024;
    private final Runtime runtime = Runtime.getRuntime();

    private long totBytes;
    private long completedFileBytes;
    private long startTime;
    private long totLines;
    private int fileNum;
    private boolean useGzip = true;


    public void processFile(File inputFile) throws IOException {
        startTime = System.currentTimeMillis();
        totBytes = inputFile.length();

        FileInputStream fs = new FileInputStream(inputFile);
        CountingInputStream cs = new CountingInputStream(fs);
        InputStream nextStream = cs;
        if (useGzip) {
            nextStream = new GZIPInputStream(nextStream);
        }
        InputStreamReader isr = new InputStreamReader(nextStream);
        BufferedReader br = new BufferedReader(isr, 256 * 1024);

        String line;
        long lastLog = System.currentTimeMillis();
        long curLine = 0;

        while ((line = br.readLine()) != null) {
            processLine(line, curLine);
            curLine++;
            totLines++;

            long now = System.currentTimeMillis();
            if (now - lastLog >= 250) {
                lastLog = now;

                long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / MB;
                long curBytes = completedFileBytes + cs.getCount();
                double elapSec = (now - startTime) / 1000.0;
                double curRate = (elapSec > 0) ? curBytes / elapSec : 0;
                double remainTime = (curRate > 0) ? (totBytes - curBytes) / curRate : 0;
                double linesPerSec = (0.0 + totLines) / elapSec;
                String durStr = DurationFormatUtils.formatDuration((long) remainTime * 1000, "H:mm:ss");
                String extraStatus = "";

                String status = String.format(
                        "File %2d   Line %,12d   Elap %,8.1f   Remain %8s   %7.3f %%   MB %,8d   Line/Sec %,8.0f   %s",
                        fileNum, totLines, elapSec, durStr, 100.0 * curBytes / totBytes, usedMB, linesPerSec,
                        extraStatus);

                System.out.println(status);
            }
        }

        br.close();
    }


    public abstract void processLine(String line, long curLine);
}
