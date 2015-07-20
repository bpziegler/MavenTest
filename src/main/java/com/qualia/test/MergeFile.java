package com.qualia.test;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


public class MergeFile implements Comparable<MergeFile> {

    private String lastLine;
    private File file;
    private int segmentId;
    private BufferedReader br;


    public MergeFile(File file) {
        this.file = file;

        String s = file.getName();
        s = s.replace("test_file_", "");
        s = s.replace(".txt", "");
        segmentId = Integer.valueOf(s);

        try {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            moveNextLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public int compareTo(MergeFile o) {
        return lastLine.compareTo(o.lastLine);
    }


    public String moveNextLine() {
        try {
            lastLine = br.readLine();
            if (lastLine == null) {
                br.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lastLine;
    }


    public String getLastLine() {
        return lastLine;
    }


    public int getSegment() {
        return segmentId;
    }

}