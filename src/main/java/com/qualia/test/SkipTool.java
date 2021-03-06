package com.qualia.test;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.google.common.base.Charsets;


/*
 * Given 2 input files, a "check" file and a "skip" file, it prints only 
 * lines from the check file that are NOT in the skip file.  Note that both files
 * MUST be sorted.
 */
public class SkipTool {

    private final String checkFile;
    private final String skipFile;
    private final String outputFile;

    private long onlyLeft = 0;
    private long onlyRight = 0;
    private long both = 0;


    public SkipTool(String checkFile, String skipFile, String outputFile) {
        this.checkFile = checkFile;
        this.skipFile = skipFile;
        this.outputFile = outputFile;
    }


    private void run() throws IOException {
        SortedFile checkSortedFile = new SortedFile(new File(checkFile));
        SortedFile skipSortedFile = new SortedFile(new File(skipFile));

        FileOutputStream fs = new FileOutputStream(outputFile);
        OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
        BufferedWriter bw = new BufferedWriter(osw);

        long startTime = System.currentTimeMillis();
        long lastLog = System.currentTimeMillis();

        while (true) {
            if ((checkSortedFile.getLastLine() == null) && (skipSortedFile.getLastLine() == null)) {
                break;
            }

            int compare = checkSortedFile.compareTo(skipSortedFile);

            if (compare < 0) {
                // This line exists only in the check file, so we should output the line
                bw.write(checkSortedFile.getLastLine());
                bw.newLine();
                checkSortedFile.moveNextLine();
                onlyLeft++;
            } else if (compare > 0) {
                skipSortedFile.moveNextLine();
                onlyRight++;
            } else {
                checkSortedFile.moveNextLine();
                skipSortedFile.moveNextLine();
                both++;
            }

            if (System.currentTimeMillis() - lastLog >= 1000) {
                lastLog = System.currentTimeMillis();
                dumpStats(startTime);
            }
        }

        bw.close();

        dumpStats(startTime);
    }


    private void dumpStats(long startTime) {
        double elap = (System.currentTimeMillis() - startTime + 0.0) / 1000;
        System.out.println(String.format("Elap %9.2f   Left %,15d   Right %,15d   Both %,15d", elap, onlyLeft,
                onlyRight, both));
    }


    public static void main(String[] args) throws IOException {
        String checkFile = args[0];
        String skipFile = args[1];
        String outputFile = args[2];
        SkipTool program = new SkipTool(checkFile, skipFile, outputFile);
        program.run();
    }

}
