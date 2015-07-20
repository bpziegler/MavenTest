package com.qualia.test;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;


public class MergeTest {
    private static String DATA_DIR = "/Users/benziegler/work/test-merge";

    private final PriorityQueue<MergeFile> priorityQueue = new PriorityQueue<MergeFile>();


    public void mergeFiles(List<File> files) throws IOException {
        long lineNum = 0;
        long startTime = System.currentTimeMillis();

        for (File file : files) {
            MergeFile mergeFile = new MergeFile(file);
            if (mergeFile.getLastLine() != null) {
                priorityQueue.add(mergeFile);
            }
        }

        String prevLine = null;
        ArrayList<Integer> segments = new ArrayList<Integer>();

        FileOutputStream fos = new FileOutputStream("merge_output");
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter outputWriter = new BufferedWriter(osw);

        while (true) {
            MergeFile mergeFile = priorityQueue.poll();
            if (mergeFile == null) {
                break;
            }

            if (!mergeFile.getLastLine().equals(prevLine) && (prevLine != null)) {
                lineNum = flushLine(lineNum, prevLine, segments, outputWriter);

                if (lineNum % 100000 == 0) {
                    printStatus(lineNum, startTime);
                }
            }

            segments.add(mergeFile.getSegment());
            prevLine = mergeFile.getLastLine();

            if (mergeFile.moveNextLine() != null) {
                priorityQueue.add(mergeFile);
            }
        }

        // Flush the last line
        if (prevLine != null && segments.size() > 0) {
            lineNum = flushLine(lineNum, prevLine, segments, outputWriter);
        }

        outputWriter.close();
        printStatus(lineNum, startTime);
    }


    private long flushLine(long lineNum, String prevLine, ArrayList<Integer> segments, BufferedWriter outputWriter)
            throws IOException {
        Collections.sort(segments);
        String s = prevLine + " " + segments.toString();
        outputWriter.write(s);
        outputWriter.write("\n");
        segments.clear();
        lineNum++;
        return lineNum;
    }


    private void printStatus(long lineNum, long startTime) {
        double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000;
        String log = String.format("Line %,10d  Elap %6.1f", lineNum, elap);
        System.out.println(log);
    }


    public static void main(String[] args) throws IOException {
        List<File> files = new ArrayList<File>();
        File dir = new File(DATA_DIR);
        for (File oneFile : dir.listFiles()) {
            if (oneFile.getName().contains("test_file_")) {
                files.add(oneFile);
            }
        }

        MergeTest mergeTest = new MergeTest();
        mergeTest.mergeFiles(files);
    }

}
