package com.qualia.util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.time.DurationFormatUtils;

import com.google.common.io.CountingInputStream;


public class MultiFileLineProcessor {

    private final int MB = 1024 * 1024;
    private final Runtime runtime = Runtime.getRuntime();

    private ILineProcessor lineProcessor;
    private long totBytes;
    private long completedFileBytes;
    private long startTime;
    private long totLines;
    private boolean useGzip;
    private long lastLog;
    private int numError;


    public void processDir(String dirPath, ILineProcessor lineProcessor) throws IOException {
        List<File> fileList = new ArrayList<File>();
        FileRecur.getFilesInDirRecursively(dirPath, fileList);
        processFiles(fileList, lineProcessor);
    }


    public void processFiles(Collection<File> files, ILineProcessor lineProcessor) throws IOException {
        this.lineProcessor = lineProcessor;
        totBytes = 0;
        totLines = 0;
        completedFileBytes = 0;
        numError = 0;
        startTime = System.currentTimeMillis();

        // First get the total size of all files to process
        for (File oneFile : files) {
            totBytes += oneFile.length();
        }

        // Now process each file
        int fileNum = 0;
        for (File oneFile : files) {
            fileNum++;
            try {
                processFile(oneFile, fileNum);
            } catch (Exception e) {
                numError += 1;
            }
            completedFileBytes += oneFile.length();
        }
    }


    private void processFile(File oneFile, int fileNum) throws IOException {
        // System.out.println("Process file:  " + oneFile.getAbsolutePath());

        FileInputStream fs = new FileInputStream(oneFile);
        CountingInputStream cs = new CountingInputStream(fs);
        InputStream nextStream = cs;
        if (isUseGzip() && oneFile.getName().endsWith(".gz")) {
            nextStream = new GZIPInputStream(nextStream);
        }
        InputStreamReader isr = new InputStreamReader(nextStream);
        BufferedReader br = new BufferedReader(isr, 256 * 1024);

        String line;
        if (lastLog == 0) {
            lastLog = System.currentTimeMillis();
        }
        long curLine = 0;

        while ((line = br.readLine()) != null) {
            lineProcessor.processLine(line, curLine);
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
                String elapStr = DurationFormatUtils.formatDuration((long) elapSec * 1000, "H:mm:ss");
                String durStr = DurationFormatUtils.formatDuration((long) remainTime * 1000, "H:mm:ss");
                String extraStatus = lineProcessor.getStatus();

                String status = String.format(
                        "File %2d   Line %,12d   Elap %8s   Remain %8s   %7.3f %%   MB %,8d   Err %,4d   Line/Sec %,8.0f   %s",
                        fileNum, totLines, elapStr, durStr, 100.0 * curBytes / totBytes, usedMB, numError, linesPerSec,
                        extraStatus);

                System.out.println(status);
            }
        }

        br.close();
    }


    public boolean isUseGzip() {
        return useGzip;
    }


    public void setUseGzip(boolean useGzip) {
        this.useGzip = useGzip;
    }

}
