package com.qualia.util;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Splitter;


public class AnalyzeNumbers {

    private static final Splitter splitter = Splitter.on(",");

    private int num = 0;
    private double tot = 0;
    private double tot_sqr = 0;
    private double max = -Double.MAX_VALUE;
    private double min = Double.MAX_VALUE;
    private List<Double> values = new ArrayList<Double>();


    private void run(String filePath) throws IOException {
        MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
        lineProcessor.processOneFile(new File(filePath), new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                AnalyzeNumbers.this.processLine(line, curLine);
            }


            @Override
            public String getStatus() {
                return AnalyzeNumbers.this.getStatus();
            }
        });
        System.out.println(getStatus());
        printHistogram();
    }


    private void printHistogram() {
        final int NUM_BUCKET = 20;
        for (int i = 0; i < NUM_BUCKET; i++) {
            int low_idx = (values.size()-1) * i / NUM_BUCKET;
            int high_idx = (values.size()-1) * (i+1) / NUM_BUCKET;
            double per = (i + 1.0) / NUM_BUCKET * 100;
            System.out.println(String.format("Bucket %2d   %5.0f %%   Value <= %9.1f", i+1, per, values.get(high_idx)));
        }
    }


    protected String getStatus() {
        Collections.sort(values);
        double median = values.get(values.size() / 2);
        double avg = (num > 0) ? tot / num : 0;
        return String.format("%,8d num   %9.1f avg   %9.1f max   %9.1f min   %9.1f median", num, avg, max, min, median);
    }


    protected void processLine(String line, long curLine) {
        double val = Double.valueOf(line);
        num += 1;
        tot += val;
        tot_sqr += (val * val);
        max = (val > max) ? val : max;
        min = (val < min) ? val : min;
        values.add(val);
    }


    public static void main(String[] args) throws IOException {
        String filePath = (args.length > 0) ? args[0] : "/Users/benziegler/work/tasks/FIDO-197/accurate_user_signal_counts.txt";
        AnalyzeNumbers program = new AnalyzeNumbers();
        program.run(filePath);
    }

}
