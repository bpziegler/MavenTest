package com.qualia.hbasetest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class LockerThread extends Thread {

    // Maximum number of *individual* locks. Some objects request more than
    // one lock, and each one counts towards this.
    public static int MAX_LOCKS = 5000;
    public static int MAX_LOCKABLES_WAITING = 1000;

    public AtomicBoolean canShutdown = new AtomicBoolean();

    // Input Queues
    public final BlockingQueue<Lockable> needLocksQueue = new ArrayBlockingQueue<Lockable>(1000);
    public final BlockingQueue<Lockable> releaseLocksQueue = new ArrayBlockingQueue<Lockable>(1000 * 10);

    // Output Queues
    public final BlockingQueue<Lockable> haveLocksQueue = new ArrayBlockingQueue<Lockable>(1000);

    // Internal
    private final List<Lockable> waiting = new ArrayList<Lockable>();
    // TODO: Change "locks" to a hashmap that records the owner and a
    // timestamp
    private final Set<String> locks = new HashSet<String>();
    private final List<Lockable> localReleaseList = new ArrayList<Lockable>();
    private long totCheckFail;
    private int maxNeedReturn;


    @Override
    public void run() {
        // Number of lockables we have sent to the output queue, but need to later release their locks
        int needReturn = 0;

        while (true) {
            // Check if we have some locks to release
            localReleaseList.clear();
            releaseLocksQueue.drainTo(localReleaseList);
            int numReleased = localReleaseList.size();

            for (Lockable lockable : localReleaseList) {
                releaseLocks(lockable);
            }
            // System.out.println("Released " + localReleaseList.size());
            localReleaseList.clear();
            needReturn -= numReleased;

            // Check if any of the waiting lockables get can their locks
            int numCheckFail = 0;
            Iterator<Lockable> waitingIter = waiting.iterator();
            while (waitingIter.hasNext()) {
                if (locks.size() >= MAX_LOCKS) {
                    break;
                }
                Lockable lockable = waitingIter.next();
                if (canGetLocks(lockable)) {
                    acquireLocks(lockable);
                    waitingIter.remove();
                    try {
                        haveLocksQueue.put(lockable); // TODO: lockables can have their own optional "put back" queue
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    needReturn++;
                    if (needReturn > maxNeedReturn) {
                        maxNeedReturn = needReturn;
                    }
                } else {
                    numCheckFail++;
                }
            }
            totCheckFail += numCheckFail;

            // TODO: If numCheckFail is above some threshold, we should use a High/Low watermark for when we resume
            // checks

            // Check if we can process lockables from the input queue
            int numAdd = 0;
            while (locks.size() < MAX_LOCKS && waiting.size() < MAX_LOCKABLES_WAITING) {
                Lockable nextLockable = needLocksQueue.poll();
                if (nextLockable == null) {
                    break;
                }
                waiting.add(nextLockable);
                numAdd++;
                // System.out.println("Add to waiting " + waiting.size());
            }

            if (needReturn == 0 && waiting.size() == 0 && needLocksQueue.size() == 0 && canShutdown.get()) {
                break;
            }

            if (numAdd == 0 && numReleased == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        
        System.out.println("totCheckFail = " + totCheckFail + "   maxNeedReturn = " + maxNeedReturn);
    }


    private void releaseLocks(Lockable lockable) {
        for (String oneLock : lockable.getNeededLocks()) {
            locks.remove(oneLock);
        }
    }


    private void acquireLocks(Lockable lockable) {
        for (String oneLock : lockable.getNeededLocks()) {
            locks.add(oneLock);
        }
    }


    private boolean canGetLocks(Lockable lockable) {
        // TODO:  Add a check that all locks within this lockable are UNIQUE
        for (String oneLock : lockable.getNeededLocks()) {
            if (locks.contains(oneLock)) {
                return false;
            }
        }

        return true;
    }

}
