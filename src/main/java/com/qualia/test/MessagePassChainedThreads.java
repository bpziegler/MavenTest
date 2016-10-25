package com.qualia.test;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


// http://benchmarksgame.alioth.debian.org/u64q/performance.php?test=threadring
public class MessagePassChainedThreads {

    private static final int NUM_THREADS = 503;

    private static class TestThread extends Thread {
        public final BlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(1);
        private final int threadNum;
        private final AtomicBoolean doneFlag;
        public TestThread nextThread;


        public TestThread(int threadNum, AtomicBoolean doneFlag) {
            this.threadNum = threadNum;
            this.doneFlag = doneFlag;
        }


        @Override
        public void run() {
            while (!doneFlag.get()) {
                try {
                    Integer msgNum = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (msgNum == null) {
                        continue;
                    }
                    if (msgNum > 0) {
                        if (msgNum % 10000 == 0) {
                            System.out.println("Passing message to next thread " + msgNum);
                        }
                        nextThread.queue.put(msgNum - 1);
                    } else {
                        System.out.println("Last thread received message!   thread = " + threadNum);
                        doneFlag.set(true);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    private void run() throws InterruptedException {
        List<TestThread> threads = new ArrayList<TestThread>();

        AtomicBoolean doneFlag = new AtomicBoolean();

        for (int i = 0; i < NUM_THREADS; i++) {
            TestThread thread = new TestThread(i, doneFlag);
            threads.add(thread);
        }

        for (int i = 0; i < threads.size(); i++) {
            if (i + 1 < threads.size()) {
                threads.get(i).nextThread = threads.get(i + 1);
            } else {
                threads.get(i).nextThread = threads.get(0);
            }
        }

        for (TestThread thread : threads) {
            thread.start();
        }

        Integer msgNum = 1000 * 1000;
        threads.get(0).queue.put(msgNum);

        for (TestThread thread : threads) {
            thread.join();
        }
    }


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        MessagePassChainedThreads program = new MessagePassChainedThreads();
        program.run();
        long elap = System.currentTimeMillis() - startTime;
        System.out.println("elap = " + elap);
    }

}
