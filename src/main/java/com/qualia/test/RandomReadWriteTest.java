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

        final RandomAccessFile simFile = new RandomAccessFile(TEST_FILE_PATH, "rw");
        simFile.setLength(simOptions.fileLen);
        final AtomicBoolean doneFlag = new AtomicBoolean(false);

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
            LOG.info(String.format("Elap %6.2f   numReads %,6d   numWrites %,6d", elap, simOptions.numReads.get(),
                    simOptions.numWrites.get()));
            Thread.sleep(100);
        }

        doneFlag.set(true);

        LOG.info("Waiting for threads to stop");
        for (Thread thread : threads) {
            thread.join();
        }

        double elap = (System.currentTimeMillis() - startTime + 0.0) / 1000;
        double readPerSec = simOptions.numReads.get() / elap;
        double writePerSec = simOptions.numWrites.get() / elap;

        LOG.info(String.format("Final stats   reads/sec = %,6.0f   writes/sec = %,6.0f", readPerSec, writePerSec));

        LOG.info("Deleting test file");
        new File(TEST_FILE_PATH).delete();
        LOG.info("Finished");
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
