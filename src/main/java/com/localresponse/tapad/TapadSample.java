package com.localresponse.tapad;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Splitter;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class TapadSample {

    private Splitter tabSplitter = Splitter.on("\t");
    private Splitter equalSplitter = Splitter.on("=");
    private Random random = new Random();

    private Set<String> idSet = new HashSet<String>();
    private BufferedWriter bw;
    private long numAdd = 0;
    private long numHit = 0;
    private long numId;
    private long idLen;
    private long totDevice;
    private long numLine;


    private void run(String tapadFile) throws IOException {
        FileOutputStream fs = new FileOutputStream("tapad_sample.txt");
        bw = new BufferedWriter(new OutputStreamWriter(fs), 128 * 1024);

        List<File> list = new ArrayList<File>();
        list.add(new File(tapadFile));
        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                TapadSample.this.processLine(line, curLine);
            }


            public String getStatus() {
                String status = TapadSample.this.getStatus();

                return status;
            }

        });

        bw.close();

        System.out.println(getStatus());
    }


    protected String getStatus() {
        return String.format("NumAdd %,d   NumHit %,d   Avg Id Len %3.1f   Avg # Device/Line %3.1f", numAdd, numHit, (idLen + 0.0) / numId, (totDevice + 0.0) / numLine);
    }


    protected void processLine(String line, long curLine) {
        boolean logLine = false;

        List<String> devices = tabSplitter.splitToList(line);
        for (String oneDevice : devices) {
            List<String> parts = equalSplitter.splitToList(oneDevice);
            String idTypeStr = parts.get(0);
            String idStr = parts.get(1);
            String platformStr = parts.get(2);
            
            numId++;
            idLen += idStr.length();

            if (random.nextDouble() < 0.00002) {
                idSet.add(idStr);
                logLine = true;
                numAdd++;
            } else {
                boolean inSet = idSet.contains(idStr);
                if (inSet) {
                    logLine = true;
                    numHit++;
                }
            }
        }
        
        numLine++;
        totDevice += devices.size();

        if (logLine) {
            try {
                bw.write(line + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String tapadFile = "/Users/benziegler/work/tapad/LocalResponse_ids_full_20140827_203357";

        if (args.length > 0) {
            tapadFile = args[0];
            System.out.println("tapadFile = " + tapadFile);
        }

        TapadSample tapadSample = new TapadSample();

        tapadSample.run(tapadFile);
    }
}
