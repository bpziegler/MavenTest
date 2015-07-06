package com.qualia.hbasetest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LockTest {

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
			for (int i = 0; i < 20 * 1000; i++) {
				TestLockable testLockable = new TestLockable();
				for (int j = 0; j < 2; j++) {
					String s = String.format("%06d", random.nextInt(100 * 1000));
					testLockable.neededLocks.add(s);
				}

				try {
					numPush++;
					lockerThread.needLocksQueue.put(testLockable);
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

		public ReceiveThread(LockerThread lockerThread) {
			super();
			this.lockerThread = lockerThread;
		}

		@Override
		public void run() {
			long start = System.currentTimeMillis();
			int numReceive = 0;
			
			while (lockerThread.isAlive()) {
				Lockable next = lockerThread.haveLocksQueue.poll();
				if (next != null) {
					numReceive++;
					try {
						lockerThread.releaseLocksQueue.put(next);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			}
			
			long elap = System.currentTimeMillis() - start;
			System.out.println("Num Receive = " + numReceive);
			System.out.println("Receive Elap = " + elap);
		}

	}

	public static void main(String[] args) throws InterruptedException {
		LockerThread lockerThread = new LockerThread();
		PushThread pushThread = new PushThread(lockerThread);
		ReceiveThread receiveThread = new ReceiveThread(lockerThread);

		lockerThread.start();
		pushThread.start();
		receiveThread.start();
		
		lockerThread.join();
		pushThread.join();
		receiveThread.join();
	}

}
