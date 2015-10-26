package com.qualia.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class SortedFile implements Comparable<SortedFile> {
    private String lastLine;
    private BufferedReader br;


    public SortedFile(File file) {
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
    public int compareTo(SortedFile o) {
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

}