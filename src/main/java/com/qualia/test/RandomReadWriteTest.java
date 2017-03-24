package com.qualia.test;


import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.qualia.test.SimReadWriteThread.SimType;


public class RandomReadWriteTest {

    private static final Logger LOG = Logger.getLogger(RandomReadWriteTest.class.getName());


    private void run(ReadWriteSimOptions simOptions) throws Exception {
        LOG.info("Starting:  " + simOptions);

        simOptions.fileLen = simOptions.fileSizeGB * 1024l * 1024l * 1024l;

        final String TEST_FILE_PATH = "test_sim_file.dat";

        final RandomAccessFile simFile = new RandomAccessFile(TEST_FILE_PATH, (simOptions.useCache) ? "rw" : "rwd");
        LOG.info("Creating test file    size = " + simOptions.fileLen);
        simFile.setLength(simOptions.fileLen);
        final AtomicBoolean doneFlag = new AtomicBoolean(false);

        LOG.info("Creating Read/Write threads");
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < simOptions.readThreads; i++) {
            SimReadWriteThread thread = new SimReadWriteThread(simOptions, SimType.READ, simFile, doneFlag);
            threads.add(thread);
            thread.start();
        }
        for (int i = 0; i < simOptions.writeThreads; i++) {
            SimReadWriteThread thread = new SimReadWriteThread(simOptions, SimType.WRITE, simFile, doneFlag);
            threads.add(thread);
            thread.start();
        }

        long startTime = System.currentTimeMillis();
        while (true) {
            double elap = (System.currentTimeMillis() - startTime + 0.0) / 1000;
            if (elap >= simOptions.simTimeSec) {
                break;
            }
            logStats(simOptions, startTime);
            Thread.sleep(100);
        }

        doneFlag.set(true);

        LOG.info("Waiting for threads to stop");
        for (Thread thread : threads) {
            thread.join();
        }
        LOG.info("Closing file");
        simFile.close();

        logStats(simOptions, startTime);

        LOG.info("Deleting test file");
        new File(TEST_FILE_PATH).delete();
        LOG.info("Finished");
    }


    private void logStats(ReadWriteSimOptions simOptions, long startTime) {
        double elap = (System.currentTimeMillis() - startTime + 0.0) / 1000;
        int numRead = simOptions.numReads.get();
        int numWrite = simOptions.numWrites.get();
        double readPerSec = numRead / elap;
        double writePerSec = numWrite / elap;

        LOG.info(String.format("Elap %6.2f   numRead %,8d   numWrite %,8d   reads/sec = %,6.0f   writes/sec = %,6.0f", elap, numRead, numWrite, readPerSec, writePerSec));
    }


    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s  %5$s%6$s%n");

        ReadWriteSimOptions simOptions = ReadWriteSimOptions.createFromCmdLine(args);
        if (simOptions == null)
            return;

        RandomReadWriteTest program = new RandomReadWriteTest();
        program.run(simOptions);
    }

}
