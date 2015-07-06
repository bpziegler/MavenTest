package com.qualia.hbasetest;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class LockTest {

    private static final int NUM_TEST = 1 * 1000 * 1000;
    private static final int RANDOM_RANGE = 1000;     // 50 = 164501 lockables/sec, 5000 = 244738 lockables/sec

    private static class TestLockable implements Lockable {

        public List<String> neededLocks = new ArrayList<String>();


        @Override
        public List<String> getNeededLocks() {
            return neededLocks;
        }

    }

    public static class PushThread extends Thread {

        private final LockerThread lockerThread;


        public PushThread(LockerThread lockerThread) {
            super();
            this.lockerThread = lockerThread;
        }


        @Override
        public void run() {
            int numPush = 0;
            Random random = new Random();
            for (int i = 0; i < NUM_TEST; i++) {
                TestLockable testLockable = new TestLockable();
                while (testLockable.neededLocks.size() < 2) {
                    String s = String.format("%010d", random.nextInt(RANDOM_RANGE));
                    if (!testLockable.neededLocks.contains(s)) {
                        testLockable.neededLocks.add(s);
                    }
                }

                try {
                    lockerThread.needLocksQueue.put(testLockable);
                    numPush++;
                    // System.out.println("push " + numPush + "   queue size = " + lockerThread.needLocksQueue.size());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            lockerThread.canShutdown.set(true);
            System.out.println("Num Push = " + numPush);
        }

    }

    public static class ReceiveThread extends Thread {
        private final LockerThread lockerThread;
        private long start;
        private long numReceive;


        public ReceiveThread(LockerThread lockerThread) {
            super();
            this.lockerThread = lockerThread;
        }


        @Override
        public void run() {
            start = System.currentTimeMillis();
            numReceive = 0;

            while (lockerThread.isAlive()) {
                Lockable next = lockerThread.haveLocksQueue.poll();
                if (next != null) {
                    numReceive++;
                    try {
                        // System.out.println("receive " + numReceive);
                        lockerThread.releaseLocksQueue.put(next);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (numReceive % 50000 == 0) {
                        dumpStats();
                    }
                }
            }

            dumpStats();
        }


        private void dumpStats() {
            double elap = (System.currentTimeMillis() - start + 0.0) / 1000;
            double rate = numReceive / elap;
            System.out.println(String.format("Receive %8d   Elap %6.1f   Rate = %6.0f lockables/sec", numReceive, elap,
                    rate));
        }

    }


    public static void main(String[] args) throws InterruptedException {
        LockerThread lockerThread = new LockerThread();
        PushThread pushThread = new PushThread(lockerThread);
        ReceiveThread receiveThread = new ReceiveThread(lockerThread);

        lockerThread.start();
        pushThread.start();
        receiveThread.start();

        System.out.println("All threads started");

        lockerThread.join();
        pushThread.join();
        receiveThread.join();

        System.out.println("Done");
    }

}
