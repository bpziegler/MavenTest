package com.qualia.test;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;


public class SimReadWriteThread extends Thread {
    private final ReadWriteSimOptions simOptions;
    private final SimType simType;
    private final RandomAccessFile simFile;
    private final AtomicBoolean doneFlag;

    public static enum SimType {
        READ, WRITE
    }


    public SimReadWriteThread(ReadWriteSimOptions simOptions, SimType simType, RandomAccessFile simFile,
            AtomicBoolean doneFlag) {
        super();
        this.simOptions = simOptions;
        this.simType = simType;
        this.simFile = simFile;
        this.doneFlag = doneFlag;
    }


    @Override
    public void run() {
        try {
            runWorker();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void runWorker() throws IOException {
        final int PAGE_SIZE = ReadWriteSimOptions.PAGE_SIZE;
        Random random = new Random();
        long pageLen = simOptions.fileLen / PAGE_SIZE;
        while (!doneFlag.get()) {
            int readSizeIdx = random.nextInt(simOptions.readSizes.size());
            int readPages = simOptions.readSizes.get(readSizeIdx) / PAGE_SIZE;
            long readPos = ((long) (Math.floor(random.nextDouble() * (pageLen - readPages)))) * PAGE_SIZE;
            simFile.seek(readPos);
            int bufLen = readPages * PAGE_SIZE;
            byte[] buf = new byte[bufLen];
            if (simType == SimType.READ) {
                int bytesRead = simFile.read(buf, 0, bufLen);
                if (bytesRead != bufLen)
                    throw new RuntimeException("bytesRead does not match " + bytesRead);
                simOptions.numReads.incrementAndGet();
            }
            else if (simType == SimType.WRITE) {
                simFile.write(buf, 0, bufLen);
                simOptions.numWrites.incrementAndGet();
            }
        }
    }

}