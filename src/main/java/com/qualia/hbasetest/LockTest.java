package com.qualia.hbasetest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockTest {

	public interface Lockable {
		public List<String> getNeededLocks();
	}

	private static class LockerThread extends Thread {

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

		@Override
		public void run() {
			// Number of lockables we have sent to the output queue, but need to later release their locks
			int needReturn = 0;
			
			while (true) {
				// Check if we have some locks to release
				releaseLocksQueue.drainTo(localReleaseList);
				int numReleased = localReleaseList.size();

				for (Lockable lockable : localReleaseList) {
					releaseLocks(lockable);
				}
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
						haveLocksQueue.add(lockable); // TODO: lockables can have their own optional "put back" queue
						needReturn++;
					} else {
					    numCheckFail++;
					}
				}
				
				// TODO:  If numCheckFail is above some threshold, we should use a High/Low watermark for when we resume checks
				
				// Check if we can process lockables from the input queue
				int numAdd = 0;
				while (locks.size() < MAX_LOCKS && waiting.size() < MAX_LOCKABLES_WAITING) {
					Lockable nextLockable = needLocksQueue.poll();
					if (nextLockable == null) {
						break;
					}
					waiting.add(nextLockable);
					numAdd++;
				}
				
				if (needLocksQueue.size() == 0 && canShutdown.get()) {
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
			for (String oneLock : lockable.getNeededLocks()) {
				if (locks.contains(oneLock)) {
					return false;
				}
			}

			return true;
		}

	}

	public static void main(String[] args) {
	}

}
