package com.qualia.test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;


public class MessagePassTest {
    
    private final ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(100000);
    private final AtomicBoolean doneFlag = new AtomicBoolean();
    private final CyclicBarrier barrier = new CyclicBarrier(2);

    private static class Sender implements Runnable {
        private ArrayBlockingQueue<Object> queue;
        private AtomicBoolean doneFlag;
        private CyclicBarrier barrier;

        public Sender(ArrayBlockingQueue<Object> queue, AtomicBoolean doneFlag, CyclicBarrier barrier) {
            this.queue = queue;
            this.doneFlag = doneFlag;
            this.barrier = barrier;
        }

        public void run() {
            try {
                barrier.await();
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
            for (int i = 0; i < 10 * 1000 * 1000; i++) {
                String s = Integer.toString(i);
                try {
                    queue.put(s);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
            System.out.println("Send thread is done");
            doneFlag.set(true);
        }
    }

    private static class Receiver implements Runnable {
        private ArrayBlockingQueue<Object> queue;
        private AtomicBoolean doneFlag;
        private CyclicBarrier barrier;

        public Receiver(ArrayBlockingQueue<Object> queue, AtomicBoolean doneFlag, CyclicBarrier barrier) {
            this.queue = queue;
            this.doneFlag = doneFlag;
            this.barrier = barrier;
        }

        public void run() {
            try {
                barrier.await();
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
            long numRecv = 0;
            while (true) {
                Object msg = queue.poll();
                if (msg != null) {
                    numRecv++;
                } else if (doneFlag.get()) {
                    break;
                }
            }
            System.out.println("Receive thread is done " + numRecv);
        }
    }


    public void run() throws InterruptedException {
        Sender sender = new Sender(queue, doneFlag, barrier);
        Receiver receiver = new Receiver(queue, doneFlag, barrier);

        Thread senderThread = new Thread(sender);
        senderThread.start();

        Thread receiverThread = new Thread(receiver);
        receiverThread.start();

        senderThread.join();
        receiverThread.join();
    }


    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("Starting " + startTime);
        new MessagePassTest().run();
        long stopTime = System.currentTimeMillis();
        System.out.println("Finished " + (stopTime - startTime));
    }

}
