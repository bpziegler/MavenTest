package com.localresponse.tapad;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Splitter;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class LargeLines {

    private Splitter tabSplitter = Splitter.on("\t");
    private int maxDevices;
    private int maxBoth;
    private int maxCookies;


    private void run(String tapadDir) throws IOException {
        File[] files = (new File(tapadDir)).listFiles();
        List<File> list = Arrays.asList(files);

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.setUseGzip(true);
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                LargeLines.this.processLine(line, curLine);
            }


            public String getStatus() {
                String status = LargeLines.this.getStatus();

                return status;
            }

        });
    }


    protected String getStatus() {
        return String.format("maxBoth = %3d   maxDevices = %3d   maxCookies = %3d", maxBoth, maxDevices, maxCookies);
    }


    protected void processLine(String line, long curLine) {
        List<String> devices = new ArrayList<String>(tabSplitter.splitToList(line));
        
        boolean newMax = false;

        if (devices.size() > maxBoth) {
            maxBoth = devices.size();
            newMax = true;
        }

        int numCookie = 0;
        int numDevice = 0;

        for (String oneDevice : devices) {
            if (oneDevice.startsWith("SUPPLIER_APPNEXUS=")) {
                numCookie++;
            } else {
                numDevice++;
            }
        }

        if (numDevice > maxDevices) {
            maxDevices = numDevice;
            newMax = true;
        }

        if (numCookie > maxCookies) {
            maxCookies = numCookie;
            newMax = true;
        }
        
        if (newMax) {
            System.out.println("Line = " + line);
        }
    }


    public static void main(String[] args) throws Exception {
        String tapadDir = null;

        if (args.length > 0) {
            tapadDir = args[0];
            System.out.println("tapadDir = " + tapadDir);
        }

        LargeLines program = new LargeLines();

        program.run(tapadDir);
    }

}
