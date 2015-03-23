package com.qualia.test;


import gnu.trove.set.hash.TLongHashSet;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class ThreadTest implements Runnable {

    private final HashFunction hashFunction = Hashing.murmur3_128(); // Simulates MD5 hash (but only 8 bytes)
    private final TLongHashSet hashSet = new TLongHashSet();

    private final AtomicBoolean done;
    private final BlockingQueue<Message> queue;


    public ThreadTest(AtomicBoolean done, BlockingQueue<Message> queue) {
        this.done = done;
        this.queue = queue;

        for (int i = 0; i < 1000 * 1000; i++) {
            hashSet.add(i);
        }
    }


    public void run() {
        try {
            run_worker();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void run_worker() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long iter = 0;
        long numEmpty = 0;
        long numExist = 0;

        while (!done.get()) {
            iter++;
            Message msg = queue.poll(1, TimeUnit.MILLISECONDS);
            if (msg == null) {
                numEmpty++;
            } else {
                // process the message
                boolean exists = hashSet.contains(msg.value);
                if (exists) {
                    numExist++;
                }
            }
        }

        long elap = System.currentTimeMillis() - startTime;
        System.out.println("elap = " + elap);
        System.out.println("iter = " + iter);
        System.out.println("numEmpty = " + numEmpty);
    }

    private static class FeederThread implements Runnable {

        private final BlockingQueue<Message> queue;
        private long nextValue = 0;


        public FeederThread(BlockingQueue<Message> queue) {
            this.queue = queue;
        }


        public void run() {
            while (true) {
                Message msg = new Message();
                msg.value = nextValue++;
                try {
                    queue.put(msg);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }


    public static void main(String[] args) throws InterruptedException {
        System.out.println("ThreadTest start");

        final AtomicBoolean done = new AtomicBoolean();
        final BlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(1000 * 1000);

        FeederThread feederThread = new FeederThread(queue);
        Thread thread2 = new Thread(feederThread);
        thread2.setDaemon(true);
        thread2.start();

        ThreadTest threadTest = new ThreadTest(done, queue);
        Thread thread = new Thread(threadTest);
        thread.start();

        System.out.println("sleeping");
        for (int i = 0; i < 5; i++) {
            System.out.println("Sleeping " + i);
            Thread.sleep(1000);
        }
        done.set(true);

        thread.join();
        System.out.println("ThreadTest end");
    }

}
